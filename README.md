reshaper
===============================

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

Relations
---------

When transferring data from a table which includes a foreign key we use SubTransformerField which only takes another transformer as argument

Example

    class ForeignTransformer(Transformer):
        field = TransformerField()
        another_field = TransformerField()

        class Meta:
            ...

    class RelatedTransformer(Transformer):
        title = TransformerField()
        foreign = SubTransformerField(ForeignTransformer)

        class Meta:
            ...

This way when RelatedTransformer is built, once it gets to
the foreign field it will transform the foreign key object, insert it to the new destination database and return the id of that row we just inserted.

How about if we want to keep the relations between models but we want to change it so that RelatedTransformer does not actually hold the foreign key but RelatedTransformer and ForeignTransformer are connected through a seperate table?

    RelatedForeignRelation

    pk | related_pk | foreign_pk
     1 |          2 |          3

To achieve this we set related\_table in SubTransformerField constructor.

    class ForeignTransformer(Transformer):
        ...

    class RelatedTransformer(Transformer):
        title = ...
        foreign = SubTransformerField(ForeignTransformer, related_table='related_foreign_relation')

This way the ForeignTransformer is built and inserted into the database, then the RelatedTransformer is built and finally the primary key's of those two are added to 'related\_foreign\_relation' table. Column foreign is no longer part of the table that just got transformed and the relations between the two only exists in the relation table.

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
    | 1 | Beach party | 20_05_2015 | Come to this party! , you'll love it|


Actions
-------

Actions run after transformation has taken place. They are almost identical to filters
but they don't alter the original value in any way, they just run whatever function
is passed taking the original value as an argument

    def done(str):
        if str:
            print('Done transforming this field')
