from enum import Enum


class Status(Enum):
    ACTIVE = 'active'
    OVERLOADED = 'overloaded'
    OFFLINE = 'offline'


class RType(Enum):
    UPDATE = 0
    QUERY = 1


class ROp(Enum):
    ADD_RATING = 'u.add_rating'
    ADD_TAG = 'u.add_tag'
    GET_MOVIE = 'q.get_movie'
    GET_RATINGS = 'q.get_ratings'
    GET_AVG_RATING = 'q.get_avg_rating'
    GET_GENRES = 'q.get_genres'
    GET_TAGS = 'q.get_tags'
    SEARCH_TITLE = 'q.search_title'
    SEARCH_GENRE = 'q.search_genre'
    SEARCH_TAG = 'q.search_tag'
