from src.reshaper.transformers import *

class AuthorTransformer(Transformer):
    name = TransformerField(destination_id='author_name')
    age  = TransformerField(destination_id='author_age')

    class Meta:
        source_table = 'author'
        destination_table = 'new_author'

class MovieTransformer(Transformer):
    title = TransformerField()
    author_id = RelationTransformerField(
        AuthorTransformer,
        destination_id='author_id',
        relation_table='movie_author'
    )

    class Meta:
        destination_id = 'movie_id'
        source_table = 'movie'
        destination_table = 'new_movie'

class ActorTransformer(Transformer):
    name = TransformerField()
    movie = RelationTransformerField(
        MovieTransformer,
        destination_id='movie_id',
        relation_table='movie_actor'
    )
    author = RelationTransformerField(
        AuthorTransformer,
        destination_id='author_id',
        relation_table='actor_author'
    )

    class Meta:
        destination_id = 'actor_id'
        source_table = 'actor'
        destination_table = 'new_actor'

class DirectorTransformer(Transformer):
    name = TransformerField()
    country_id = SubTransformerField(
        fk_table = 'new_country'
    )

    class Meta:
        source_table = 'director'
        destination_table = 'new_director'
