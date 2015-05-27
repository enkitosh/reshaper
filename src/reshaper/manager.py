class Manager:
    def __init__(self, source_db=None, destination_db=None):
        self.source_db = source_db
        self.destination_db = destination_db
        self.transformers = []
        self.cache = {}

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
            self.cache = {}

            self.cache['relation_table'] = subtransformer_field.relation_table
            column_name = key
            if subtransformer_field.destination:
                column_name = subtransformer_field.destination
            self.cache[column_name] = pk
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

        if not len(self.cache) == 0:
            self.cache[transformer.source_table + '_id'] = pk
            self.destination_db.insert_single(
                self.cache.pop('relation_table'), self.cache
            )
            self.cache = {}
        return pk

    def transform(self, transformer):
        source_table = transformer.source_table
        rows = self.source_db.get_table_rows(source_table)
        for row in rows:
            transformed = transformer.run_transformations(row)
            self.insert(transformer, transformed)

    def transformAll(self):
        for transformer in self.transformers:
            self.transform(transformer)
