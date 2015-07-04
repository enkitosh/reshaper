reshaper 
===============================

[ ![Codeship Status for dv_dev/reshaper](https://codeship.com/projects/f1369680-df30-0132-9ebc-767a4e17443c/status?branch=master)](https://codeship.com/projects/80493) [![codecov.io](http://codecov.io/bitbucket/dv_dev/reshaper/coverage.svg?branch=master&token=MFw2iuSbb7)](http://codecov.io/bitbucket/dv_dev/reshaper?branch=master)

A tool to transfer data from one database to another.
The transformation of data is done with Transformers.
In a transformer you can declare a source table to transfer
data and a destination table where fields will be inserted.

Example

    from reshaper.transformers import Transformer, TransformerField

    class ArticleTransformer(Transformer):
        main_text = TransformerField()

        class Meta:
            source_table = 'news_entry'
            destination_table = 'articles_article'

Note that reshaper has no idea about django or the django ORM, or what Models are so source\_table/destination\_table will have to have identical names to how they are named within the source/destination database

In the example above we are transforming a single field "main\_text". This field has the same name as the field it's pulling from the source\_database. If you want the destination table to have another name you can do this by declaring the name of the new column in TransformerField constructor like this:

    main_text = TransformerField(destination='text')

The new column is now inserted to the destination database with the name text instead of main\_text

Foreign keys
---------

When transforming tables that include foreign keys where that foreign key relation must be preserved use SubTransformerField.

Example

    class PersonTransformer(Transformer):
        name = TransformerField()
        profile_id = SubTransformerField(
            ProfileTransformer,
            destination_id='pid'
        )

By default all fields have the optional argument create=True.
This means that in the example above, when the transformer hits profile\_id, it starts by insterting profile into the destination database, given the destination table from meta information of ProfileTransformer. It will then replace the value of profile\_id with the new primary key. This is done recursively so even if ProfileTransformer includes other fields which are also SubTransformerFields they will be built before finally returning the primary key of the new profile.

Passing a transformer with SubTransformerField is optional.
If you don't provide a transformer that field is expected
to have a foreign key relation to a row already stored in 
the destination database. In this situation, looking up
the correct foreign key in the destination database might be
hard because when transfering data, primary keys might end up not being the same if you don't specially declare id as a field in 
your transformer. In this case you can use the unique meta field

Example:

    class UserTransformer:
        ...
        class Meta:
            unique = 'username'

This just means that within the table used as a source for
UserTransformer the column username is unique. When a transformer
includes this meta field the manager fetches the
row from the original source by id, gets the value of the unique column and then looks up that row by field/value in the destination database to get the correct primary key value.


If no unique field is provided the manager will just use the same primary key as was found in the source database. Currently this is done without validating weither this foreign key is the same in the destination database, so make sure you verify that all primary keys map out correctly from source to destination.

Relations
---------

RelationTransformerFields work similar to SubTransformerFields but there is a fundamental difference between the two. You use SubTransformerField when you want to keep the same foreign key in the table from source to destination but RelationTransformerField when you want to make a connection between two or more objects in a relation table. 

Let's say I have a User table, and in my old database users relation to their profile was stored as a foreign key but now I want users to be able to have multiple profiles, so I want to make a relation table

    | pk | user_id | profile_id |
      1  |       1 |          1 |
      2  |       2 |          2 |


    The transformer

    class User(Transformer):
        profile_id = RelationTransformerField(
            transformer=ProfileTransformer,
            relation_table='user_profile'
        )

        class Meta:
            # To specify name of the transformer itself in relation table
            destination_id = 'user_id

Like noted above the transformer will create the profile in the destination database if created is not altered. So it will create this profile, return the new pk but not insert it but put this connection in a storage resolved after the user object has been inserted into the database. After that has happened the manager checks if the user object has any relations in storage, if so it inserts the new user primary key and the relation primary key into the destination table specified in "relation\_table"
            
Filters
--------

Every TransformerField takes the optional argument "filters"
which is a list of functions to be applied to the value corresponding to that field. Lets say we have this in database

    table party
  
      pk        title         date       invitation_str
    | 1 | Beach party | 20_05_2015 | Come to this party! |

Now if we have the function

    def fun(str):
        return str + ' , you'll love it!'

We can apply it as a filter

    class BeachPartyTransformer(Transformer):
        ...
        invitation_str = TransformerField(filters=[fun])
        
        class Meta:
            source_table = 'party'
            destination_table = 'beach_parties'

This would result in

      pk        title         date                         invitation_str
    | 1 | Beach party | 20_05_2015 | Come to this party! , you'll love it!|


Actions
-------

Actions run after transformation has taken place. They are almost identical to filters
but they don't alter the original value in any way, they just run whatever function
is passed taking the original value as an argument

    def done(str):
        if str:
            print('Done transforming this field')
