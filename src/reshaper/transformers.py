class Field:   
    def __init__(
        self, 
        destination_id=None, 
        filters=[],
        actions=[],
        create=True):
        """
        All fields have:
        :param str destination_id: The name of the db column after its value has been transformed
        :param list filter: List of functions to run on the value being transformed
        :param list actions: List of functions to run on the value being transformed without altering that value
        """
        self.destination_id = destination_id
        self.filters = filters
        self.actions = actions
        self.create = create

    def get_type(self):
        return self.__class__.__name__.lower()

    def transform(self, data):
        cname = data.get('key')
        if self.destination_id:
            cname = self.destination_id
        value = data.get('value')
        if self.filters:
            for filter in self.filters:
                value = filter(value)
        if self.actions:
            for action in self.actions:
                action(value)

        return {
            'key': cname,
            'value': value
        }


class TransformerField(Field):
    """
    TransformerField is used to map a field 
    from source to destination
    """
    def __init__(
        self, 
        destination_id=None, 
        filters=[], 
        actions=[],
        create=True
    ):
        """ TransformerField constructor 
        :param str destination_id: Name of transformed column 
        :param list filters: A list of functions that alter the original value 
        :param list actions: A list of functions to run after transformation
        """
        super(TransformerField, self).__init__(
            destination_id=destination_id,
            filters=filters,
            actions=actions,
            create=create
        )

class RelationTransformerField(Field):
    def __init__(
        self, 
        transformer=None, 
        relation_table=None,
        destination_id=None, 
        filters=[], 
        actions=[],
        create=True
    ):
        """
        Specifies relation between two models in a destination table
        :param Transformer transformer: Transformer object
        :param str relation_table: Name of destination table
        :param str destination_id: Name of destination column
        :param list filters: A list of functions that alter the original value 
        :param list actions: A list of functions to run after transformation
        """
        super(RelationTransformerField, self).__init__(
            destination_id=destination_id,
            filters=filters,
            actions=actions,
            create=create
        )
        self.transformer = transformer
        self.relation_table = relation_table

class SubTransformerField(Field):
    def __init__(
            self, 
            transformer=None,
            fk_table=None,
            source_table=None,
            destination_id=None, 
            filters=[],
            actions=[],
            create=True
        ):

        """
        Field used to preserve foreign key relations
        when an object relies on a foreign key being 
        translated from source database to destination database.
        SubTransformerField can take a transformer as argument.
        If no transformer is passed the value of the source_db
        column is used as a lookup in fk_table.
        That is, if no transformer is passed in foreign keys
        are expected to be identical from source database
        to destination database even if their table names vary.
        
        :param Transformer transformer: Transformer object(optional)
        :param str fk_table: Table where foreign key is stored
        :param str destination_id: Name of destination column
        :param list filters: A list of functions that alter the original value 
        :param list actions: A list of functions to run after transformation

        """
        super(SubTransformerField, self).__init__(
            destination_id=destination_id,
            filters=filters,
            actions=actions,
            create=create
        )
        self.transformer=transformer
        self.fk_table = fk_table
        self.source_table = source_table

class ValueField(Field):
    def __init__(
        self, 
        value, 
        destination_id=None, 
        filters=[],
        actions=[],
        create=True):
        super(ValueField, self).__init__(
            destination_id = destination_id,
            filters=filters,
            actions=actions,
            create=create
        )
        self.value = value

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
        for name, value in attrs.items():
            if isinstance(value, TransformerField)\
            or isinstance(value, RelationTransformerField)\
            or isinstance(value, SubTransformerField)\
            or isinstance(value, ValueField):
                transformer_fields[name] = value
            setattr(new_class, name, value)
        # Check for Meta options
        if attr_meta:
            setattr(new_class, '_meta', attr_meta.__dict__)
        # TransformerFields declared within Transformer instance
        if transformer_fields:
            setattr(new_class, '_fields', transformer_fields)
        return new_class


class Transformer(metaclass=TransformerMeta):
    """
    Transformer handles transforming original values
    to new values depending on what is declared within their
    TransformerFields. 
        - destination_id     = Name of table if being 
                               connected with a relation table
        - source_table       = Name of source table in db
        - destination_table  = Name of destination table in db
    """
    def __init__(self, *args, **kwargs):
        self.source_table = self._meta.get('source_table', None)
        self.destination_table = self._meta.get('destination_table', None)
        self.destination_id = self._meta.get('destination_id', None)
        self.unique = self._meta.get('unique', None)

    def run_transformations(self, data):
        """
        Run transformation for a single row from source db

        :param dict row: Database row with values to transform
        :return: Transformed row
        :rtype: dict
        """
        transformed = {} 
        for key in self._fields.keys():
            value = ''
            transform_field = self._fields[key]
            if key in data.keys():
                value = data[key]
            else:
                if transform_field.get_type() == 'valuefield':
                    value = self._fields[key].value

            transformed[key] = {
                'field' : transform_field,
                'data': transform_field.transform({
                    'key': key,
                    'value': value
                })
            }
        return transformed
