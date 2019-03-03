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
                print(f'Invalid movie rating [ {rating} ]". ',
                      'Rating must be between 0 - 5.')
                print('- ' * 32)
                rating = input('Enter movie rating (0 - 5): ')
                continue
            elif rating % 0.5 != 0:
                rating = round(rating * 2) / 2
                print('Your rating was rounded to the nearest 0.5.')
            break
        except ValueError:
            print('- ' * 32)
            print(f'Invalid movie rating [ {rating} ]". ',
                  'Rating must be a number.')
            print('- ' * 32)
            rating = input('Enter movie rating (0 - 5): ')
            continue

    return rating


def format_search_result(result, search_var, search_val):
    if result:
        n = len(result)
        titles = "\n".join([row['title'] for row in result])
        response = (f'Results for {search_var} "{search_val}" ({n} results):\n'
                    f'{titles}')
    else:
        response = f'No results for {search_var} "{search_val}"'

    return response


def print_menu():
    print()
    print(' --- Movie Database ---')
    print()
    [print(option) for option in menu_options]
    print()
    print(f' {len(menu_options) + 1}. Exit')
    print()
    print('Enter option: ', end='')


def main():
    userId = get_user_id()

    while True:
        request = None
        response = None

        print_menu()
        choice = input()
        print()

        if choice == '1':
            op = ROp.ADD_RATING.value
            title = get_title()
            rating = get_rating()
            request = (op, userId, title, rating)
            try:
                result = frontend.send_request(request)
                response = f'{result}\n\nYou have rated {title} a {rating}/5'
            except Exception as e:
                response = e

        elif choice == '2':
            op = ROp.ADD_TAG.value
            title = get_title()
            tag = get_tag()
            request = (op, userId, title, tag)
            try:
                result = frontend.send_request(request)
                response = f'{result}\n\nYou have tagged {title} with "{tag}"'
            except Exception as e:
                response = e

        elif choice == '3':
            op = ROp.GET_AVG_RATING.value
            title = get_title()
            request = (op, title)
            try:
                result = frontend.send_request(request)
                response = (f'Average rating for {title}: '
                            f'{round(result, 1)}/5')
            except Exception as e:
                response = e

        elif choice == '4':
            op = ROp.GET_RATINGS.value
            title = None
            print('Choose a movie you want to see your rating of,',
                  'or leave it blank to view all of your ratings.')
            title = get_title()
            request = (op, userId, title)
            try:
                result = frontend.send_request(request)
                if result:
                    response = ' Title'.ljust(50, ' ') + '| Rating\n'
                    response += '-' * 65 + '\n'
                    for row in result:
                        if len(row['title']) > 50:
                            response += row['title'][:48] + '- | '
                            response += row['rating'] + '\n -'
                            response += row['title'][48:].ljust(48, ' ')
                            response += '|\n'
                        else:
                            response += row['title'].ljust(50, ' ') + '| '
                            response += row['rating'] + '\n'

                else:
                    response = 'You have submitted no ratings yet.'
            except Exception as e:
                response = e

        elif choice == '5':
            op = ROp.GET_GENRES.value
            title = get_title()
            request = (op, title)
            try:
                result = frontend.send_request(request)
                result = "\n".join(result)
                response = f'Genres for {title}:\n{result}'
            except Exception as e:
                response = e

        elif choice == '6':
            op = ROp.GET_TAGS.value
            title = get_title()
            request = (op, title)
            try:
                result = frontend.send_request(request)
                result = "\n".join(result)
                response = f'Tags for {title}:\n{result}'
            except Exception as e:
                response = e

        elif choice == '7':
            op = ROp.SEARCH_TITLE.value
            title = get_title()
            request = (op, title)
            try:
                result = frontend.send_request(request)
                response = format_search_result(result, 'title', title)
            except Exception as e:
                response = e

        elif choice == '8':
            op = ROp.SEARCH_GENRE.value
            genre = get_genre()
            request = (op, genre)
            result = frontend.send_request(request)
            response = format_search_result(result, 'genre', genre)

        elif choice == '9':
            op = ROp.SEARCH_TAG.value
            tag = get_tag()
            request = (op, tag)
            try:
                result = frontend.send_request(request)
                response = format_search_result(result, 'tag', tag)
            except Exception as e:
                response = e

        elif choice == '10':
            print('Bye!')
            break

        else:
            print('- ' * 32)
            print(f'Invalid option [ {choice} ]. ',
                  f'Enter an option from 1 - {len(menu_options) + 1}.')
            print('- ' * 32)
            continue

        print()
        print(response)
        print()
        input('Press ENTER to continue.')


if __name__ == '__main__':
    try:
        with Pyro4.locateNS() as ns:
            try:
                uri = ns.lookup('network.frontend')
            except Pyro4.errors.NamingError:
                print('Could not find front end server, exiting.')
                exit(1)
            frontend = Pyro4.Proxy(uri)

        main()
    except Pyro4.errors.NamingError:
        print('Could not find Pyro nameserver, exiting.')
