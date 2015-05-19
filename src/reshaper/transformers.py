class TransformerField:
    """
    TransformerField is used to map a field 
    from source to destination
        - destination : The name of the destination column
                        If not specified the column will
                        keep the same name.
        - filters : A list of functions that alter the original value
        - actions : A list of functions to run after transformation
    """
    def __init__(self, destination=None, filters=[], actions=[]):
        self.destination = destination
        self.filters = filters
        self.actions = actions

class SubTransformerField:
    """
    Transformerfield to specify a relation between two models
    This is for foreign keys. If a table has a foreign key
    A transformer must be constructed and passed in through this
    field. The transformer will then identify this is a foreign key,
    build the related object with its own transformer and return 
    the id of the newly built object
    """
    def __init__(self, transformer):
        self.transformer = transformer

class TransformerMeta(type):
    """
    Metaclass for transfomer class
    """
    def __new__(cls, name, bases, attrs):
        super_new = super(TransformerMeta, cls).__new__
        module = attrs.pop('__module__')
        new_class = super_new(
            cls, name, bases, {'__module__':module}
        )
        attr_meta = attrs.pop('Meta', None)

        transformer_fields = {}
        transformer_relations = {}
        for name, value in attrs.items():
            if isinstance(value, TransformerField):
                transformer_fields[name] = value
            elif isintance(value, SubTransformerField):
                transformer_relations[name] = value
            setattr(new_class, name, value)
        if attr_meta:
            setattr(new_class, '_meta', attr_meta.__dict__)
        if transformer_fields:
            setattr(new_class, '_fields', transformer_fields)
        if transformer_relations:
            setattr(new_class, '_relations', transformer_relations)
        return new_class

class Transformer(metaclass=TransformerMeta):
    def __init__(self, *args, **kwargs):
        self.source = self._meta.get('source')
        if not self.source:
            raise Exception('Name of source db table must be specified')
        self.destination = self._meta.get('destination', None)
        self.conn = None

    def build_single(self, id):
        row = self.conn.get_row_from_pk(self.source, id)
        values = self.run_transformations(row)
        # Insert values get pk from db
        # return pk
        return 0

    def apply_filters(self, filters, values):
        print("Applying filters")
        for f in filters:
            values = list(map(f, values))
        return values

    def run_transformations(self, row, fields):
        transformed = {}
        for column in row.keys():
            transform_field = fields.get(column)
            if not transform_field:
                if column in self._relations:
                    fk = self._relations.get(column)
                    fk_object = fk.build_single(row.get(column))
                transformed[fk_object.get('name')] = fk_object.get('pk')
                else:
                    transformed[column] = row[column]
            else:
                # Check if name of destination column differs
                field_name = column
                if transform_field.destination:
                    field_name = transform_field.destination
                transformed[field_name] = row[column]

                # Check for filters and run them
                if transform_field.filters:
                    for filter in transform_field.filters:
                        transformed[field_name] = filter(
                            transformed[field_name]
                        )

                # Check for actions and run them
                if transform_field.actions:
                    for action in transform_field.actions:
                        action(transformed[field_name])
        return transformed


    def transform(self):
        transformations = []
        if not self._fields:
            raise Exception('No transformer fields specified')
        else:
            rows = self.conn.get_table_rows(self.source)
            for row in rows:
                transformations.append(
                    self.run_transformations(
                        row, self._fields
                    )
                )
        return transformations
