from .transformers import *

class Manager:
    def __init__(
        self, 
        source_db=None, 
        destination_db=None
    ):
        self.source_db = source_db
        self.destination_db = destination_db
        self.transformers = [] 
        self.cache = []

    def add_transformer(self, transformer):
        self.transformers.append(transformer)

    def get_from_unique(
            self, 
            table, 
            unique, 
            value, 
            db='source_db'
    ):
        dest_db = self.source_db
        if db == 'destination_db':
            dest_db = self.destination_db

        return dest_db.get_row_from_field(
            table, unique, value
        )

    def resolve_unique(self, transformer, unique_value):
        dest_row = self.get_from_unique(
            transformer.destination_table,
            transformer.unique,
            unique_value,
            db='destination_db'
        )

        return dest_row


    def add_relation(self, table, data):
        """
        Adds relation data to table cache
        """
        self.cache.append({table:data})

    def resolve_relationtransformerfield(
            self, 
            field, 
            value, 
            transformers
        ):
        """
        Resolve a RelationTransformerField
        """

        # A single transformer can have a relation
        # with multiple elements
        if type(transformers) is not list:
            transformers = [transformers]

        for transformer in transformers:
            if transformer.source_table:
                row = self.source_db.get_row_from_pk(
                    transformer.source_table,
                    value
                )
                transformer.set_values(row)

            if transformer.method == 'get_or_create':
                # If the transformer declares a get_or_create
                # method we lookup that unique value in 
                # source db and perform a lookup
                # with the results on the destination db
                if transformer.unique:
                    unique_value = row.get(transformer.unique)
                    data = self.resolve_unique(
                        transformer,
                        unique_value
                    )
                else:
                    raise Exception(
                        'No unique declared for transformer: %s' % transformer.__class__.__name__
                    )
            else:
                data = self.insert(transformer)

            if not transformer.commit:
                self.add_relation(
                    field.relation_table,
                    data
                )

            else:
                self.add_relation(
                    field.relation_table,
                    {transformer.destination_id: data.get('id')}
                )

        # This function always returns 0
        # Relations are stored and resolved
        # once the main transformer has been
        # inserted into the database and returns an id
        return 0

    def resolve_subtransformerfield(self, field, value, transformer):
        """
        Resolve SubTransformerField
        """
        pk = value

        row = transformer.to_dict()

        if transformer.source_table:
            row = self.source_db.get_row_from_pk(
                transformer.source_table,
                value
            )
            transformer.set_values(row)

            if field.commit:
                pk = self.insert(transformer).get(field.key)
            else:
                pk = row.get(field.key)

        if transformer.unique:
            unique_value = row.get(transformer.unique)
            dest_row = self.resolve_unique(
                transformer, unique_value
            )
            pk = dest_row.get(field.key)

        return pk

    def insert(self, transformer):
        """
        Insert a single row from resolved transformer data

        :param Transformer transformer: Transformer object

        :return: A dictionary containing id: primary_key if data was commited, otherwise a dictionary containing transformed columns
        """
        pk = None
        transformed = {}
        for key, value in transformer.to_dict().items():
            field = transformer.to_field(key)
            if field and value:
                if isinstance(field, RelationTransformerField):
                    pk = self.resolve_relationtransformerfield(
                        field,
                        value,
                        field.transform(transformer)
                    )
                elif isinstance(field, SubTransformerField):
                    pk = self.resolve_subtransformerfield(
                        field,
                        value,
                        field.transform(transformer)
                    )
                    transformed[key] = pk
                elif isinstance(field, TransformerField):
                    if field.commit:
                        transformed[key] = value if value else ""
                elif isinstance(field, ValueField):
                    transformed[key] = field.value
        if transformer.commit:
            pk = self.destination_db.insert_single(
                transformer.destination_table, transformed
            )
            return pk
        else:
            return transformed

    def transform(self, transformer, row):
        """
        Performs transformation of all fields declared
        in transformer on a single row

        :param Transformer transformer: Transformer object
        :param dict row: Dictionary containing row values from source database
        """
        transformer.set_values(row)
        pk = self.insert(transformer).get('id')

        if self.cache:
            for relation in self.cache:
                for key,value in relation.items():
                    table = key
                    value[transformer.destination_id] = pk
                    self.destination_db.insert_single(
                        table, value
                    )
            self.cache = []
        return pk

    def transformAll(self, transformers):
        for transformer in transformers:
            self.transform(transformer)
