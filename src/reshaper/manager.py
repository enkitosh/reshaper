class Manager:
    def __init__(self, source_db=None, destination_db=None):
        self.source_db = source_db
        self.destination_db = destination_db
        self.transformers = []

    def add_transformer(self, transformer):
        self.transformers.append(transformer)

    def insert(self,transformer, row):
        relations = []
        for key,value in row.items():
            if key in transformer._related.keys():
                rel = transformer._related.get(key)
                # Build foreign object
                pk = self.insert(
                    rel.transformer,
                    self.source_db.get_row_from_pk(
                        transformer.source_table,
                        value
                    )
                )
                if not rel.related_table:
                    row[key] = pk
                else:
                    relations.append({
                        'pk' : pk,
                        'related_table' : rel.related_table
                    })

        obj_pk = self.destination_db.insert_single(
            transformer.destination_table,
            row
        )

        if relations:
            for relation in relations:
                self.destination_db.add_relation(
                    relation.get('related_table'),
                    relation.get('pk'),
                    obj_pk
                )
        return obj_pk

    def transform(self, transformer):
        source_table = transformer.source_table
        dest_table = transformer.destination_table
        rows = self.source_db.get_table_rows(source_table)
        for row in rows:
            transformed = transformer.run_transformations(row)
            self.insert(transformer, transformed)

    def transformAll(self):
        for transformer in self.transformers:
            self.transform(transformer)
