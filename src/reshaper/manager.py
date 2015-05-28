class Manager:
    def __init__(self, source_db=None, destination_db=None):
        self.source_db = source_db
        self.destination_db = destination_db
        self.transformers = []
        self.cache = []

    def add_transformer(self, transformer):
        self.transformers.append(transformer)

    def resolve_subtransformer(self, subtransformer_field, key, value):
        transformer = subtransformer_field.transformer()

        row = self.source_db.get_row_from_pk(
            transformer.source_table, value
        )

        new_row = transformer.run_transformations(row)
        pk = self.insert(transformer, new_row)
        if subtransformer_field.relation_table:
            relations = {}

            relations['relation_table'] = subtransformer_field.relation_table
            column_name = key
            if subtransformer_field.destination_id:
                column_name = subtransformer_field.destination_id
            relations[column_name] = pk
            self.cache.append(relations)
            return 0
        else:
            return pk

    def insert(self, transformer, row):
        removed = []
        for key,value in row.items():
            if  hasattr(transformer, '_relations')\
            and key in transformer._relations:
                pk = self.resolve_subtransformer(**value)
                if pk != 0:
                    row[key] = pk
                else:
                    removed.append(key)

        for remove in removed:
            row.pop(remove)

        pk = self.destination_db.insert_single(
            transformer.destination_table, row
        )

        return pk

    def transform(self, transformer):
        source_table = transformer.source_table
        rows = self.source_db.get_table_rows(source_table)
        for row in rows:
            transformed = transformer.run_transformations(row)
            pk = self.insert(transformer, transformed)
            if self.cache:
                for relation in self.cache:
                    table = relation.pop('relation_table')
                    relation[transformer.destination_id] = pk
                    self.destination_db.insert_single(
                        table, relation
                    )
                self.cache = []

    def transformAll(self):
        for transformer in self.transformers:
            self.transform(transformer)
