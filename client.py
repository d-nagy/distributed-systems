import Pyro4
from enums import ROp


frontend = None
menu_options = [
    ' 1. Rate a movie',
    ' 2. Add a tag to a movie',
    ' 3. Get the average rating for a movie',
    ' 4. Get your ratings',
    ' 5. Get the genres for a movie',
    ' 6. Get the tags for a movie',
    ' 7. Search movies by title',
    ' 8. Search movies by genre',
    ' 9. Search movies by tag'
]


def get_user_id():
    uid = input('Enter a user ID (number): ')

    while not uid.isdigit():
        print('- ' * 32)
        print(f'Invalid user ID [ {uid} ]". User ID must be a number.')
        print('- ' * 32)
        uid = input('Enter a user ID (number): ')

    return int(uid)


def get_title():
    title = input('Enter movie title: ').lower()
    return title


def get_tag():
    tag = input('Enter a tag for the movie: ')
    return tag


def get_genre():
    genre = input('Enter a movie genre: ').lower()
    return genre


def get_rating():
    rating = input('Enter movie rating (0 - 5): ')

    while True:
        try:
            rating = float(rating)
            if not 0 <= rating <= 5:
                print('- ' * 32)
                print(f'Invalid movie rating [ {rating} ]". \
                        Rating must be between 0 - 5.')
                print('- ' * 32)
                rating = input('Enter movie rating (0 - 5): ')
                continue
            elif rating % 0.5 != 0:
                rating = round(rating * 2) / 2
                print('Your rating was rounded to the nearest 0.5.')
            break
        except ValueError:
            print('- ' * 32)
            print(f'Invalid movie rating [ {rating} ]". \
                    Rating must be a number.')
            print('- ' * 32)
            rating = input('Enter movie rating (0 - 5): ')
            continue

    return rating


def format_search_result(result, search_var, search_val):
    if result:
        n = len(result)
        titles = [row['title'] for row in result]
        response = f'Results for {search_var} "{search_val}" ({n} results):\n \
                    {"\n".join(titles)}'
    else:
        response = f'No results for {search_var} "{search_val}"'

    return response


def print_menu():
    print()
    print(' --- Movie Database ---')
    print()
    [print(option) for option in menu_options]
    print()
    print(f' {len(menu_options + 1)}. Exit')
    print()
    print('Enter option: ')


def main():
    userId = get_user_id()

    while True:
        request = None
        response = None

        choice = input(print_menu())

        if choice == '1':
            op = ROp.ADD_RATING.value
            title = get_title()
            rating = get_rating()
            request = (op, userId, title, rating)
            result = frontend.send_request(request)
            response = f'{result}\n\nYou have rated {title} a {rating}/5'

        elif choice == '2':
            op = ROp.ADD_TAG.value
            title = get_title()
            tag = get_tag()
            request = (op, userId, title, tag)
            result = frontend.send_request(request)
            response = f'{result}\n\nYou have tagged {title} with "{tag}"'

        elif choice == '3':
            op = ROp.GET_AVG_RATING.value
            title = get_title()
            request = (op, title)
            result = frontend.send_request(request)
            response = f'Overall average rating for {title}: {result}/5'

        elif choice == '4':
            op = ROp.GET_RATINGS.value
            title = None
            print('Choose a movie you want to see your rating of, \
                   or leave it blank to view all of your ratings.')
            title = get_title()
            request = (op, title)
            result = frontend.send_request(request)
            if result:
                if title:
                    response = f'Your rating of {title}:\n \
                                {"\n".join(result)}'
                else:
                    response = f'Your ratings ({len(result)} results):\n \
                                {"\n".join(result)}'
            else:
                response = f'You have submitted no ratings yet.'

        elif choice == '5':
            op = ROp.GET_GENRES.value
            title = get_title()
            request = (op, title)
            result = frontend.send_request(request)
            response = f'Genres for {title}:\n{"\n".join(result)}'

        elif choice == '6':
            op = ROp.GET_TAGS.value
            title = get_title()
            request = (op, title)
            result = frontend.send_request(request)
            response = f'Tags for {title}:\n{"\n".join(result)}'

        elif choice == '7':
            op = ROp.SEARCH_TITLE.value
            title = get_title()
            request = (op, title)
            result = frontend.send_request(request)
            response = format_search_result(result, 'title', title)

        elif choice == '8':
            op = ROp.SEARCH_GENRE.value
            genre = get_genre()
            request = (op, genre)
            result = frontend.send_request(request)
            response = format_search_result(result, 'genre', genre)

        elif choice == '8':
            op = ROp.SEARCH_TAG.value
            tag = get_tag()
            request = (op, tag)
            result = frontend.send_request(request)
            response = format_search_result(result, 'tag', tag)

        elif choice == '10':
            print('Bye!')
            break

        else:
            print('- ' * 32)
            print(f'Invalid option [ {choice} ]. \
                    Enter an option from 1 - {len(menu_options) + 1}.')
            print('- ' * 32)
            continue

        print()
        print(response)
        print()


if __name__ == '__main__':
    with Pyro4.locateNS() as ns:
        uri = ns.lookup('network.frontend')
        frontend = Pyro4.Proxy(uri)

    main()
