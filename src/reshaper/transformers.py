class Field:   
    def __init__(
        self, 
        source,
        transformer=None,
        filters=[],
        actions=[],
        create=True):
        """
        All fields have:
        :param str source: Name of column as it appears in source_database
        :param Transformer transformer: Transformer being used to transform source to destination
        :param list filter: List of functions to run on the value being transformed
        :param list actions: List of functions to run on the value being transformed without altering that value
        :param bool create: Indicates if the row being related to should be created or if it is already in database,
        """
        self.source = source
        self.transformer = transformer
        self.filters = filters
        self.actions = actions
        self.create = create

    def transform(self, transformer):
        return self.transformer()

class TransformerField(Field):
    """
    TransformerField is used to map a field 
    from source to destination.
    TransformerField does really transform anything
    It simply carries the value from source database
    to the destination database.
    """
    def __init__(
        self, 
        source=None,
        filters=[], 
        actions=[]
    ):
        """ TransformerField constructor 
        :param str source: Name of source column
        :param list filters: A list of functions that alter the original value 
        :param list actions: A list of functions to run after transformation
        """
        super(TransformerField, self).__init__(
            source=source,
            filters=filters,
            actions=actions
        )

class RelationTransformerField(Field):
    def __init__(
        self, 
        source,
        transformer=None, 
        relation_table=None,
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
        :param bool create: Indicates if the row being related to should be created or if it is already in database,
        """
        super(RelationTransformerField, self).__init__(
            source,
            transformer=transformer,
            filters=filters,
            actions=actions,
            create=create
        )
        self.relation_table = relation_table

class SubTransformerField(Field):
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
        :param str destination_id: Name of destination column
        :param list filters: A list of functions that alter the original value 
        :param list actions: A list of functions to run after transformation

        """
        pass

class ValueField(Field):
    """
    ValueField is used for constants not provided in the source
    database.
    Example:

        key = ValueField("value")

    Would insert the column key with the value: "value"
    into the destination database
    """
    def __init__(self, value):
        super(ValueField, self).__init__(
            '__value__'
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

        source = {}
        attr_meta = attrs.pop('Meta', None)

        transformer_fields = {}
        for name, value in attrs.items():
            setattr(new_class, name, value)
            if isinstance(value, TransformerField)\
            or isinstance(value, RelationTransformerField)\
            or isinstance(value, SubTransformerField)\
            or isinstance(value, ValueField):
                transformer_fields[name] = value
            if hasattr(value, 'source'):
                if value.source != '__value__':
                    field = {
                        'name' : name,
                        'field' : value
                    }
                    if source.get(value.source):
                        source[value.source].append(field)
                    else:
                        source[value.source] = [field]
                    val = ''
                else:
                    val = value.value
                setattr(new_class, name, val)
        # Check for Meta options
        if attr_meta:
            setattr(new_class, '_meta', attr_meta.__dict__)
        if transformer_fields:
            setattr(new_class, '_fields', transformer_fields)
        if source:
            setattr(new_class, '_source', source)
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
        if hasattr(self, '_meta'):
            self.source_table = self._meta.get('source_table', None)
            self.destination_table = self._meta.get('destination_table', None)
            self.destination_id = self._meta.get('destination_id', None)
            self.unique = self._meta.get('unique', None)
            self.commit = self._meta.get('commit', True)

    def to_dict(self):
        """
        Returns column/value of transformer as a dictionary
        """
        dic = {}
        for key in self._fields.keys():
            dic[key] = getattr(self, key)
        return dic

    def to_field(self, key):
        """
        Given a key corresponding to attribute of transformer  
        return its field

        :param str key: Key to lookup
        """
        return self._fields.get(key)

    def apply_filters(self, field, value):
        val = value
        if field.filters:
            for filter in field.filters:
                val = filter(value)
        return val

    def apply_actions(self, field, value):
        if field.actions:
            for action in field.actions:
                action(value)
            
    def set_values(self, data):
        """
        Sets values of transformer
        Runs through filters/actions of each field
        if they are specified
        """
        for key, value in data.items():
            if key in self._source.keys():
                for field in self._source[key]:
                    field_instance = field.get('field')
                    val = self.apply_filters(field_instance, value)
                    self.apply_actions(field_instance, value)
                    setattr(self, field.get('name'), val)
