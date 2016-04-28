class Field:   
    def __init__(
        self, 
        source=None,
        commit=True,
        transformer=None,
        filters=[],
        actions=[]
    ):
        """
        All fields have:
        :param str source: Name of column as it appears in source_database
        :param boolean commit: Indicates if the field being transformed should be inserted into destination db or only built
        :param Transformer transformer: Transformer being used to transform source to destination
        :param list filter: List of functions to run on the value being transformed
        :param list actions: List of functions to run on the value being transformed without altering that value
        """
        self.source = source
        self.transformer = transformer
        self.filters = filters
        self.actions = actions
        self.commit = commit

    def transform(self, transformer):
        return self.transformer()
    
    def apply_filters(self, value, n=0):
        """
        Applies field filters to value.
        Filters are applied in the same order
        they are passed in.

        :param str value: Field value
        """
        if not self.filters:
            return

        val = self.filters[n](value)
        if len(self.filters)-1 == n:
            return val
        else:
            n += 1
            return self.apply_filters(val, n)
    
    def run_actions(self, value):
        """
        Runs actions on a field.
        Actions do not alter the value of a field
        they simply run with value as argument

        :param str value: Value from field in source db
        """
        if field.actions:
            for action in field.actions:
                action(value)

class TransformerField(Field):
    """
    TransformerField is used to map a field 
    from source to destination.
    TransformerField does not really transform anything
    It simply carries the value from source database
    to the destination database.
    """
    pass

class RelationTransformerField(Field):
    def __init__(
        self, 
        source,
        commit=True,
        transformer=None, 
        relation_table=None,
        filters=[], 
        actions=[]
    ):
        """
        Specifies relation between two models in a destination table

        :param str source: Name of column as it appears in source_database
        :param boolean commit: Indicates if the field being transformed should be inserted into destination db or only built
        :param Transformer transformer: Transformer object
        :param str relation_table: Name of destination table
        :param list filters: A list of functions that alter the original value 
        :param list actions: A list of functions to run after transformation
        """
        super(RelationTransformerField, self).__init__(
            source,
            commit=commit,
            transformer=transformer,
            filters=filters,
            actions=actions
        )
        self.relation_table = relation_table

class SubTransformerField(Field):
        """
        Field used to preserve foreign key relations
        :param str source: Name of column as it appears in source_database
        :param boolean commit: Indicates if the field being transformed should be inserted into destination db or only built
        :param Transformer transformer: Transformer object
        :param str key: The key (column) to look up
        :param list filters: A list of functions that alter the original value 
        :param list actions: A list of functions to run after transformation
        """
    def __init__(
        self,
        source,
        commit=True,
        transformer=None,
        key='id',
        filters=[],
        actions=[]
    ):
        super(SubTransformerField, self).__init__(
            source,
            commit=commit,
            transformer=transformer,
            filters=filters,
            actions=actions
        )
        self.key = key


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
    """
    def __init__(self, *args, **kwargs):

        if hasattr(self, '_meta'):

            # Name of source table in source db
            self.source_table = self._meta.get(
                'source_table', None
            )
            # Name of destination table in destination db
            self.destination_table = self._meta.get(
                'destination_table', 
                None
            )
            # Name in destination db if the transformer itself is being added (in a relation table or in as a foreign key)
            self.destination_id = self._meta.get('destination_id', None)
            # Unique identifier in source db
            self.unique = self._meta.get('unique', None)

            # Insert transformed row into destination db
            # If this is false then the transformed row
            # is returned
            self.commit = self._meta.get('commit', True)

            # method: only get_or_create at the moment
            self.method = self._meta.get('method', None)

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
                    val = value
                    if field_instance.filters:
                        val = field_instance.apply_filters(value)
                    if field_instance.actions:
                        field_instance.run_actions(value)
                    setattr(self, field.get('name'), val)
