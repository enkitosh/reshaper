class TransformerField:
    """
    TransformerField is used to map a field 
    from source to destination
    """
    def __init__(self, destination=None, filters=[], actions=[]):
        """
        TransformerField constructor
        :param str destination: Name of transformed column
        :param list filters: A list of functions that alter the original value
        :param list actions: A list of functions to run after transformation
        """
        self.destination = destination
        self.filters = filters
        self.actions = actions

class SubTransformerField:
    """
    Specifies relation between two models
    This is for foreign keys. If a table has a foreign key
    A transformer must be constructed and passed in through this
    field. The transformer will then identify this is a foreign key,
    build the related object with its own transformer and return 
    the id of the newly built object
    """
    def __init__(self, transformer, related_table=None):
        """
        SubTransformerField constructor
        :param Transformer transformer: Transformer object
        :param str related_table: Related table for foreign key / transform table
        """
        self.transformer = transformer
        self.related_table = related_table

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
        # Check for Meta options
        if attr_meta:
            setattr(new_class, '_meta', attr_meta.__dict__)
        # TransformerFields declared within Transformer instance
        if transformer_fields:
            setattr(new_class, '_fields', transformer_fields)
        # SubTransformerFields declared within Transformer instance
        if transformer_relations:
            setattr(new_class, '_relations', transformer_relations)
        return new_class

class Transformer(metaclass=TransformerMeta):
    """
    Transformer handles transforming original values
    to new values depending on what is declared within their
    TransformerFields. 
    Transformers should include a Meta class with options
        - source_table       = Name of source table in db
        - destination_table  = Name of destination table in db
    """
    def __init__(self, *args, **kwargs):
        self.source_table = self._meta.get('source_table', None)
        self.destination_table = self._meta.get('destination_table', None)

    def run_transformations(self, row):
        """
        Run transformation for a single row from source db

        :param dict row: Database row with values to transform
        :return: Transformed row
        :rtype: dict
        """
        transformed = {}
        for column in row.keys():
            transform_field = self._fields.get(column)
            if not transform_field:
                # Check if this is a foreign key
                if self._related.get(column):
                    # If so we pass it the field itself
                    transformed[column] = self._related.get(column)
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
