from src.reshaper.transformers import *

class AuthorTransformer(Transformer):
    author_name = TransformerField('name')
    author_age = TransformerField('age')

    class Meta:
        destination_id = 'author_id'
        source_table = 'author'
        destination_table = 'new_author'

class MovieTransformer(Transformer):
    title = TransformerField('title')
    author_id = RelationTransformerField(
        'author_id',
        transformer=AuthorTransformer,
        relation_table='movie_author'
    )

    class Meta:
        destination_id = 'movie_id'
        source_table = 'movie'
        destination_table = 'new_movie'

class ActorTransformer(Transformer):
    name = TransformerField('movie')
    movie_id = RelationTransformerField(
        'movie_id',
        transformer=MovieTransformer,
        relation_table='movie_actor'
    )
    author_id = RelationTransformerField(
        'author_id',
        transformer=AuthorTransformer,
        relation_table='actor_author'
    )

    class Meta:
        destination_id = 'actor_id'
        source_table = 'actor'
        destination_table = 'new_actor'

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


class OwnerTransformer(Transformer):
    name = TransformerField()
    age  = TransformerField()

    class Meta:
        destination_table = 'fruit_owner'

class OwnerTransformerField(SubTransformerField):
    def transform(self, transformer):
        trans = self.transformer()
        trans.name = transformer.owner_id
        trans.age = 7

        return trans

class FruitTransformer(Transformer):
    fruit = TransformerField('fruit')
    owner_id = OwnerTransformerField(
        'owner',
        transformer=OwnerTransformer
    )

    class Meta:
        source_table = 'old_fruits'
        destination_table = 'new_fruits'
