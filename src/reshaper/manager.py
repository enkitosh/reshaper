from progressbar import ProgressBar, Bar, Percentage

class Manager:
    def __init__(self, source_db=None, destination_db=None):
        self.source_db = source_db
        self.destination_db = destination_db
        self.transformers = []
        self.cache = []
        self.stats = False
        self.mwidgets = [Bar('=','[',']'),' ',Percentage()]

    def add_transformer(self, transformer):
        self.transformers.append(transformer)

    def get_from_unique(self, table, unique, value, db='source_db'):
        dest_db = self.source_db
        if db == 'destination_db':
            dest_db = self.destination_db

        return dest_db.get_row_from_field(
            table, unique, value
        )

    def resolve_relationtransformerfield(self, field, values):
        """
        Resolve a RelationTransformerField
        """
        new_row = []
        transformer = field.transformer()
        relations = {}
        relation_table = None
        if field.relation_table:
            relation_table = field.relation_table

        if transformer.source_table:
            row = self.source_db.get_row_from_pk(
                transformer.source_table, values.get('value')
            )
            new_row = transformer.run_transformations(row)
            if field.create:
                pk = self.insert(transformer, new_row)
            else:
                # check if transformer has a unique
                if transformer.unique:
                    value = row.get(transformer.unique)
                    gfu = self.get_from_unique(
                        transformer.source_table,
                        transfomer.unique,
                        value,
                        db='destination_db'
                    )
                    pk = gfu.get('id')
                else:
                    pk = row.get('id')

            column_name = values.get('key')
            relations[relation_table]= [{column_name : pk}]
        else:
            new_row = field.transform(values)
            if field.relation_table:
                relation_table = field.relation_table
                relations[relation_table] = new_row

        self.cache.append(relations)
        return 0

    def resolve_subtransformerfield(self, field, values):
        """
        Resolve SubTransformerField (foreign keys)
        If a transformer is passed to SubTransformer
        that transformer is resolved recursively 
        until we receive the id of the transformed
        and created foreign key
        """
        pk = 0
        db = self.source_db if field.transformer\
                            else self.destination_db
        if values.get('value') and field.source_table:
            row = db.get_row_from_pk(
                field.source_table, values.get('value')
            )
            id = row.pop('id')
            if field.transformer:
                transformer = field.transformer()
                transformed_r = transformer.run_transformations(row)
                if field.create:
                    pk = self.insert(transformer, transformed_r)
                else:
                    if transformer.unique:
                        value = row.get(transformer.unique)
                        gfu = self.get_from_unique(
                            field.fk_table,
                            transformer.unique,
                            value,
                            db='destination_db'
                        )
                        pk = gfu.get('id')
            else:
                pk = id
        else:
            pk = values.get('value')
        return pk

    def insert(self, transformer, row):
        """
        Insert a single row from resolved transformer data

        :param Transformer transformer: Transformer object
        :param dict row: Transformed data
        """
        pk = None
        transformed = {}
        for key, value in row.items():
            data = value.get('data')
            field = value.get('field')
            
            if field.get_type() == 'relationtransformerfield':
                pk = self.resolve_relationtransformerfield(
                    field,
                    data
                )

            elif field.get_type() == 'subtransformerfield':
                pk = self.resolve_subtransformerfield(
                    field,
                    data
                )
                transformed[data.get('key')] = pk

            elif field.get_type() == 'transformerfield'\
            or   field.get_type() == 'valuefield':
                transformed[data.get('key')] = data.get('value')

        pk = self.destination_db.insert_single(
            transformer.destination_table, transformed
        )
        return pk

    def transform(self, transformer):
        """
        Performs transformation of all fields declared
        in transformer

        :param Transformer transformer: Transformer object
        """
        count = 0
        source_table = transformer.source_table
        rows = self.source_db.get_table_rows(source_table)
        if self.stats:
            pbar = ProgressBar(
                widgets=self.mwidgets, 
                maxval=len(rows)
            ).start()
            print("%s - Transforming %i objects" % (
                transformer.__class__.__name__,len(rows)
            ))
        for row in rows:
            row.pop('id')
            transformed = transformer.run_transformations(row)
            pk = self.insert(transformer, transformed)
            if self.cache:
                for relation in self.cache:
                    for key,value in relation.items():
                        table = key
                        for v in value:
                            v[transformer.destination_id] = pk
                            self.destination_db.insert_single(
                                table, v
                            )
                self.cache = []
            if self.stats:
                count += 1
                pbar.update(count)

        if self.stats:
            pbar.finish()

    def transformAll(self):
        for transformer in self.transformers:
            self.transform(transformer)
