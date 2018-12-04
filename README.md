reshaper 
========

![Reshaper](https://31.media.tumblr.com/c8d469a7edb6e368c939650cca52780e/tumblr_nnrxu0RIzc1ron07wo1_500.gif)

[ ![Codeship Status for naglalakk/reshaper](https://app.codeship.com/projects/cf7abaf0-da3a-0136-acc9-02f2182c4567/status?branch=master)](https://app.codeship.com/projects/317524)

[![codecov](https://codecov.io/gh/naglalakk/reshaper/branch/master/graph/badge.svg)](https://codecov.io/gh/naglalakk/reshaper)

## Installation

    pip install git+https://github.com/enkitosh/reshaper

## Usage

Reshaper enables you to take database tables from a source database and transform them into 
desired table schema in your destination database. Reshaper offers various tools to make this process easier.

### Transformers

Transformers represent a declaration on how we want the end result to be.
Consider the following table

    name    varchar(250)
    age     integer
    pet     varchar(50)

Let's say this table already has some values

    name : Bobby
    age  : 27
    pet  : Dog

If we wanted to transfer this table from source database to destination database 
we would write a transformer like so:

    from reshaper.transformers import Transformer

    PersonTransformer(Transformer):
        name = TransformerField('name')
        age  = TransformerField('age')
        pet  = TransformerField('pet')

        class Meta:
            source_table = 'person_table'
            destination_table = 'new_person_table'

The attributes of a transformer represent the columns of the destination table, altough they don't really do that all the time, I will get to why later.

A transformer includes a Meta class describing which source table the transformer should be collecting values from and a destination_table to insert the transformed results.

In the example above the table schema from source to destination database did not really change at all.


### Fields

Data from the source database can be transformed in multiple different ways using various Fields.
The basic Field, which all other fields inherit from includes:

    - source : Name of the column as it appears in source database
    - commit : Indicates weither transformed result should be inserted into destination database
    - transformer : Transformer used to transform input value
    - filters : List of functions that alter the input value
    - actions : List of functions to run while input is transformed (do not alter input value)

#### TransformerField

TransformerField is used to pass a value from source database to a destination database.
It's value is not altered by other transformers in any way, even though TransformerField technically takes a transformer as an argument, it will ignore it completely. The only way to alter the value being passed into a TransformerField is by using filters

#### SubTransformerField

SubTransformerField is used when a database table being migrated to a new table schema has a relation to another table, think foreign keys. Consider the following

	class CountryTransformer(Transformer):
		name = TransformerField('name')
		class Meta:
			source_table = 'country'
			destination_table = 'new_country'
			unique = 'name'

	class DirectorTransformer(Transformer):
		name = TransformerField('name')
		country_id = SubTransformerField(
			'country_id',
			transformer=CountryTransformer,
			commit=False
		)

		class Meta:
			source_table = 'director'
			destination_table = 'new_director'

Here the DirectorTransformer has a single SubTransformerField. When DirectorTransformer is run
reshaper will first transform the country information by looking up the id of the source table (country) and transform that row according to the restrictions of CountryTransformer. It will then pass on the primary key of that transformed row as it was inserted into the destination database.

Note that the Meta class of each transformer can take a method argument

	class Meta:
		method = 'get_or_create'
		unique = 'name'

If this is declared we are telling reshaper to not transform and insert the foreign key row, but that one exists already. The unique argument is used to declare which field we can lookup in the destination database to find the corresponding foreign key object.

To demonstrate

	source             destination
	---------------- | ---------------
	id: 1            | id: 2
	name: Bobby      | name: Bobby
	
	PersonTransformer(Transformer):
		name = TransformerField('name')
		
		class Meta:
			unique = 'name'
			method = 'get_or_create'

	FakeTransformer(Transformer):
		person = SubTransformerField('person_id', PersonTransformer)

	When transforming

		1. Get the first row from source database with id=1
		2. The field person is a SubTransformerField declaring get_or_create with unique
		3. Pass id=1 to PersonTransformer, reshaper looks up the row in the destination db by using the unique column of the resulting row from source database (id=1, name=Bobby)
		4. SELECT (pk) FROM destination_table WHERE name='Bobby';
		5. Set the value of field to the already created row, found in the destination database


#### RelationTransformerField

When database relations start to get more complex RelationTransformerField is what you want to use.

For example, if in our old database we had this:

	name: Bobby
	age: 25
	status: happy

Now let's say that we want to keep status in a seperated table, that only links status ids with person ids

	# Status table
	id:1 
	mood: happy

	--
	id: 2
	mood: grumpy

	etc..

To do this we would write something like this:

	StatusTransformer(Transformer):
		mood = TransformerField('mood')

		class Meta:
			destination_table = 'status'

	StatusTransformerField(RelationTransformerField):
		def transform(self, transformer):
			status_transformer = StatusTransformer()
			status_transformer.mood = transformer.status

			return status_transformer

	PersonTransformer(Transformer):
		name = TransformerField('name')
		age  = TransformerField('age')
		status = StatusTransformerField(
			'status',
			relation_table = 'person_status'
		)
	
		class Meta:
			destination_id = 'person_id'
			source_table = 'person'
			destination_table = 'new_person'

This looks a bit complicated but isn't really.
See every field within reshaper has a method declared called transform.
This method is the one that is called when a column is being transformed.

	def transform(self, transformer)

The transform method actually takes in a transformer as an argument. This is the transformer currently being transformed. So for example

	class PersonTransformer(Transformer):
		name = TransformerField('name')
		age  = TransformerField('age')

Both fields get passed the PersonTransformer in their transform method.
So if we are transforming a single row with the values

	name: Bobby
	age: 27

We could access them inside the transform function of each field

	(name = TransformerField('name'))

	def transform(self, transformer):
		transformer.name => 'Bobby'
		transformer.age  => 27

For a transformer to be able to establish this kind of relation to another foreign key, 
the destination_id meta attribute must be set. This tells reshaper what the identity
of the table being transformed should be called when being related to with a primary key.
The end result of our current table would be something like this

	Table: person_status

	person_id  |  status_id
		    1  |          1
            2  |          1


#### ValueField

Valuefield only passes the value assigned to it to the column in the destination table.
For example

	class PersonTransformer(Transformer):
		name = ValueField('Bobby')

For each person being transformed the value for name in the destination database would always be Bobby.

### Filters

Filters can be used to alter the input value from the source database. For example, let's say
that for some absurd reason we would have to add the prefix 'comrade' to all person names when
migrating to our new database.

	def comrade(value):
		return 'comrade_%s' % value

	class PersonTransformer(Transformer):
		name = TransformerField('name', filters=[comrade])

Note that filters have a kind of piping effect, so if we would add

	def after(value):
		return '%s_1' % value

	filters = [comrade, after] -> 'comrade_Bobby_1'

### Actions

Actions are identical to filters but they don't alter the value of the fields in any way.
They simply run on each transformation.

### Running transformations

To run transformations you would first setup your transformers and then ask runner to 
run them.

	from reshaper.runner import Runner

	# Runner takes a few arguments, all of which can be placed in a .env file
	runner = Runner(
		source_db = $SOURCE_DATABASE,
		destination_db = $DESTINATION_DATABASE,
		cache = False (default)
	)

	runner.run(PersonTransformer()) => Starts running all transformations for person table


### Using with cache

Migrating huge database tables can be a slow and tedious process. To enable transfer of data where the process is interrupted you can use the redis cache configured in runner to cache the last primary key of the source table being pulled by the transformer. This way if you stop the process you will still be able to transform from the last primary key being used to transform data instead of having to start from the scratch.

NOTE: redis config has a default redis connection = localhost on port 6379, db = 0


### Backends

Currently postgres is the only active backend, located in reshaper.backends.postgresql

### Development

	Pull this repo (git clone https://github.com/enkitosh/reshaper)
	cd reshaper && virtualenv -p python3.4 venv
	source venv/bin/activate
	python setup.py install
	pip install -r requirements.txt

	# Running tests
	green test

### Contributing

Feel free to fork this repo or make a pull request. If you decide to add a new feature
please make sure that this feature is fully tested. There is plenty of fake test data
to play around with in test/test_data
