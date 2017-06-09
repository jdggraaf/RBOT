#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import time
import random
from threading import Lock
from timeit import default_timer

from pgoapi import PGoApi
from pgoapi.exceptions import AuthException, BannedAccountException

from .fakePogoApi import FakePogoApi
from .utils import (generate_device_info, equi_rect_distance,
                    parse_new_timestamp_ms)
from .proxy import get_new_proxy

log = logging.getLogger(__name__)


class TooManyLoginAttempts(Exception):
    pass


class InvalidLogin(Exception):
    pass


# Create the API object that'll be used to scan.
def setup_api(args, status, account):
    # Create the API instance this will use.
    if args.mock != '':
        api = FakePogoApi(args.mock)
    else:
        identifier = account['username'] + account['password']
        device_info = generate_device_info(identifier)
        account['device_info'] = device_info
        api = PGoApi(device_info=device_info)

    # New account - new proxy.
    if args.proxy:
        # If proxy is not assigned yet or if proxy-rotation is defined
        # - query for new proxy.
        if ((not status['proxy_url']) or
                ((args.proxy_rotation is not None) and
                 (args.proxy_rotation != 'none'))):

            proxy_num, status['proxy_url'] = get_new_proxy(args)
            if args.proxy_display.upper() != 'FULL':
                status['proxy_display'] = proxy_num
            else:
                status['proxy_display'] = status['proxy_url']

    if status['proxy_url']:
        log.debug('Using proxy %s', status['proxy_url'])
        api.set_proxy({
            'http': status['proxy_url'],
            'https': status['proxy_url']})

    return api


# Use API to check the login status, and retry the login if possible.
# Request GET_PLAYER data to ensure that account is working.
# Request LEVEL_UP_REWARDS to accept account level up rewards.
def check_login(args, account, api, position, proxy_url):

    # Logged in? Enough time left? Cool!
    if api._auth_provider and api._auth_provider._ticket_expire:
        remaining_time = api._auth_provider._ticket_expire / 1000 - time.time()
        if remaining_time > 60:
            log.debug(
                'Credentials remain valid for another %f seconds.',
                remaining_time)
            return

    # Try to login. Repeat a few times, but don't get stuck here.
    num_tries = 0
    # One initial try + login_retries.
    while num_tries < (args.login_retries + 1):
        try:
            if proxy_url:
                api.set_authentication(
                    provider=account['auth_service'],
                    username=account['username'],
                    password=account['password'],
                    proxy_config={'http': proxy_url, 'https': proxy_url})
            else:
                api.set_authentication(
                    provider=account['auth_service'],
                    username=account['username'],
                    password=account['password'])
            break
        except BannedAccountException:
            account['banned'] = True
            log.error('Account %s is banned from Pokemon Go.',
                      account['username'])
            break
        except AuthException:
            num_tries += 1
            log.error(
                ('Failed to login to Pokemon Go with account %s. ' +
                 'Trying again in %g seconds.'),
                account['username'], args.login_delay)
            time.sleep(args.login_delay)

    if num_tries > args.login_retries:
        log.error(
            ('Failed to login to Pokemon Go with account %s in ' +
             '%d tries. Giving up.'),
            account['username'], num_tries)
        raise TooManyLoginAttempts('Exceeded login attempts.')

    # 1 - Make an empty request to mimick real app behavior.
    try:
        time.sleep(random.uniform(1.7, 2.9))
        request = api.create_request()
        request.call()
    except Exception as e:
        log.error('Login for account %s failed. Exception in call request: %s',
                  account['username'], repr(e))
        raise InvalidLogin('Unable to make first empty request.')

    # 2 - Get Player request.
    time.sleep(random.uniform(.6, 1.1))
    responses = request_get_player(api, account, True)
    if not responses or not parse_get_player(account, responses):
        raise InvalidLogin('Unable to get player information.')

    if account['warning']:
        log.warning('Account %s has received a warning.', account['username'])

    # 3 - Download Remote Config Version request.
    old_config = account['remote_config']
    time.sleep(random.uniform(.5, 0.9))
    uint_app_version = int(args.api_version.replace('.', '0'))
    response = request_download_settings(api, account, uint_app_version)
    if not response or not parse_download_settings(account, response):
        if account['banned']:
            log.warning('Account %s is probably banned.', account['username'])
            raise InvalidLogin('Received status code 3: account is banned.')
        raise InvalidLogin('Unable to retrieve download settings hash.')

    if not parse_inventory(api, account, response['responses']):
        raise InvalidLogin('Unable to retrieve player inventory.')

    # 4 - Get Asset Digest request.
    config = account['remote_config']
    if config['asset_time'] > old_config.get('asset_time', 0):
        i = random.randint(0, 3)
        result = 2
        page_offset = 0
        page_timestamp = 0
        while result == 2:
            responses = request_get_asset_digest(
                api, account, uint_app_version, page_offset, page_timestamp)
            log.debug('Getting asset digest - offset: %d.', page_offset)
            if i > 2:
                time.sleep(random.uniform(1.4, 1.6))
                i = 0
            else:
                i += 1
                time.sleep(random.uniform(.3, .5))
            try:
                response = responses['GET_ASSET_DIGEST']
                result = response['result']
                page_offset = response['page_offset']
                page_timestamp = response['timestamp_ms']
            except KeyError:
                break

    # 5 - Download Item Templates request.
    if config['template_time'] > old_config.get('template_time', 0):
        i = random.randint(0, 3)
        result = 2
        page_offset = 0
        page_timestamp = 0
        while result == 2:
            responses = request_download_item_templates(
                api, account, page_offset, page_timestamp)
            log.debug('Downloading item templates - offset: %d.', page_offset)
            if i > 2:
                time.sleep(random.uniform(1.4, 1.6))
                i = 0
            else:
                i += 1
                time.sleep(random.uniform(.3, .5))
            try:
                response = responses['DOWNLOAD_ITEM_TEMPLATES']
                result = response['result']
                page_offset = response['page_offset']
                page_timestamp = response['timestamp_ms']
            except KeyError:
                break

    # Check tutorial completion.
    if not all(x in account['tutorials'] for x in (0, 1, 3, 4, 7)):
        log.debug('Completing tutorial steps for %s.', account['username'])
        complete_tutorial(api, account)
    else:
        log.debug('Account %s already did the tutorials.', account['username'])

    # 6 - Get Player Profile request.
    time.sleep(random.uniform(.6, 1.1))
    if not request_get_player_profile(api, account, True):
        log.warning('Account %s failed to retrieve player profile.',
                    account['username'])
        raise InvalidLogin('Unable to retrieve player profile.')

    # 7 - Check if there are level up rewards to claim.
    time.sleep(random.uniform(.4, .7))
    responses = request_level_up_rewards(api, account, True)

    if not parse_level_up_rewards(api, account, responses):
        log.warning('Account %s failed to collect level up rewards.',
                    account['username'])
        raise InvalidLogin('Unable to verify player level up rewards.')

    '''
    # 8 - Make an empty request to retrieve store items.
    try:
        time.sleep(random.uniform(.6, 1.1))
        request = api.create_request()
        request.get_store_items()
        request.call()
    except Exception as e:
        log.error('Failed to get store items for account %s: %s',
                  account['username'], repr(e))
        raise InvalidLogin('Unable to request store items.')
    '''
    # Incubate eggs on available incubators.
    incubate_eggs(api, account)

    log.debug('Login with account %s was successful.', account['username'])
    time.sleep(random.uniform(12, 17))


# Complete minimal tutorial steps.
# API argument needs to be a logged in API instance.
# TODO: Check if game client bundles these requests, or does them separately.
def complete_tutorial(api, account):
    tutorial_state = account['tutorials']
    if 0 not in tutorial_state:
        time.sleep(random.uniform(1, 5))
        if request_mark_tutorial_complete(api, account, 0):
            log.debug('Account %s completed tutorial 0.', account['username'])

    if 1 not in tutorial_state:
        time.sleep(random.uniform(5, 12))
        request = api.create_request()
        request.set_avatar(player_avatar={
            'hair': random.randint(1, 5),
            'shirt': random.randint(1, 3),
            'pants': random.randint(1, 2),
            'shoes': random.randint(1, 6),
            'avatar': random.randint(0, 1),
            'eyes': random.randint(1, 4),
            'backpack': random.randint(1, 5)
        })
        log.debug('Sending set random player character request for %s.',
                  account['username'])
        request.call()

        time.sleep(random.uniform(0.3, 0.5))
        if request_mark_tutorial_complete(api, account, 1):
            log.debug('Account %s completed tutorial 1.', account['username'])

    time.sleep(random.uniform(0.5, 0.6))
    request = api.create_request()
    request.get_player_profile()
    log.debug('Fetching player profile for %s...', account['username'])
    request.call()

    starter_id = None
    if 3 not in tutorial_state:
        time.sleep(random.uniform(1, 1.5))
        request = api.create_request()
        request.get_download_urls(asset_id=[
            '1a3c2816-65fa-4b97-90eb-0b301c064b7a/1477084786906000',
            'aa8f7687-a022-4773-b900-3a8c170e9aea/1477084794890000',
            'e89109b0-9a54-40fe-8431-12f7826c8194/1477084802881000'])
        log.debug('Grabbing some game assets.')
        request.call()

        time.sleep(random.uniform(1, 1.6))
        request = api.create_request()
        request.call()

        time.sleep(random.uniform(6, 13))
        request = api.create_request()
        starter = random.choice((1, 4, 7))
        request.encounter_tutorial_complete(pokemon_id=starter)
        log.debug('Catching the starter for %s.', account['username'])
        request.call()

        time.sleep(random.uniform(0.5, 0.6))
        responses = request_get_player(api, account, False, False)
        inventory = responses.get('GET_INVENTORY', {}).get(
            'inventory_delta', {}).get('inventory_items', [])
        for item in inventory:
            pokemon = item.get('inventory_item_data', {}).get('pokemon_data')
            if pokemon:
                starter_id = pokemon.get('id')

    if 4 not in tutorial_state:
        time.sleep(random.uniform(9, 15))
        request = api.create_request()
        request.claim_codename(codename=account['username'])
        log.debug('Claiming codename for %s.', account['username'])
        request.call()

        time.sleep(random.uniform(1.1, 1.7))
        if not request_get_player(api, account, False, False):
            log.error('Tutorial step 4 failed to get player information.')

        time.sleep(random.uniform(0.13, 0.25))
        if request_mark_tutorial_complete(api, account, 4):
            log.debug('Account %s completed tutorial 4.', account['username'])

    if 7 not in tutorial_state:
        time.sleep(random.uniform(4, 6))
        if request_mark_tutorial_complete(api, account, 7):
            log.debug('Account %s completed tutorial 7.', account['username'])

    if starter_id:
        time.sleep(random.uniform(4, 5))
        request = api.create_request()
        request.set_buddy_pokemon(pokemon_id=starter_id)
        log.debug('Setting buddy pokemon for %s.', account['username'])
        request.call()
        time.sleep(random.uniform(0.8, 1.5))

    # Sleeping before we start scanning to avoid Niantic throttling.
    log.debug('And %s is done. Wait for a second, to avoid throttle.',
              account['username'])
    time.sleep(random.uniform(1.5, 2.5))
    return True


def reset_account(account):
    account['start_time'] = time.time()
    account['remote_config'] = {}
    account['last_timestamp_ms'] = int(time.time())
    account['last_active'] = None
    account['last_location'] = None
    account['failed'] = False
    account['warning'] = None
    account['banned'] = False
    account['tutorials'] = []
    account['max_items'] = 350
    account['max_pokemons'] = 250
    account['items'] = {}
    account['pokemons'] = {}
    account['incubators'] = {}
    account['eggs'] = {}
    account['used_pokestops'] = {}
    account['level'] = 0
    account['experience'] = 0
    account['encounters'] = 0
    account['throws'] = 0
    account['catches'] = 0
    account['spins'] = 0
    account['walked'] = 0.0
    account['session_experience'] = 0
    account['session_throws'] = 0
    account['session_catches'] = 0
    account['session_spins'] = 0
    account['hour_experience'] = 0
    account['hour_throws'] = 0
    account['hour_catches'] = 0
    account['hour_spins'] = 0


def cleanup_account_stats(account, pokestop_timeout):
    elapsed_time = time.time() - account['start_time']

    # Just to prevent division by 0 errors, when needed
    # set elapsed to 1 millisecond
    if elapsed_time == 0:
        elapsed_time = 1

    xp_h = account['session_experience'] * 3600.0 / elapsed_time
    throws_h = account['session_throws'] * 3600.0 / elapsed_time
    catches_h = account['session_catches'] * 3600.0 / elapsed_time
    spins_h = account['session_spins'] * 3600.0 / elapsed_time

    account['hour_experience'] = xp_h
    account['hour_throws'] = throws_h
    account['hour_catches'] = catches_h
    account['hour_spins'] = spins_h

    # Refresh visited pokestops that were on timeout.
    used_pokestops = dict(account['used_pokestops'])
    for pokestop_id in account['used_pokestops']:
        last_attempt = account['used_pokestops'][pokestop_id]
        if (last_attempt + pokestop_timeout) < time.time():
            del used_pokestops[pokestop_id]
    account['used_pokestops'] = used_pokestops


def parse_get_player(account, responses):
    try:
        player_data = responses['GET_PLAYER']['player_data']

        account['warning'] = responses['GET_PLAYER'].get('warn', None)
        account['banned'] = responses['GET_PLAYER'].get('banned', False)
        account['tutorials'] = player_data.get('tutorial_state', [])
        account['max_items'] = player_data.get('max_item_storage', 350)
        account['max_pokemons'] = player_data.get('max_pokemon_storage', 250)
        return True

    except Exception as e:
        log.error('Exception parsing player information: %s.', repr(e))

    return False


def parse_download_settings(account, response):
    try:
        # Check if account is banned.
        status_code = response['status_code']
        if status_code == 3:
            account['banned'] = True
            return False

        responses = response['responses']
        remote_config = responses['DOWNLOAD_REMOTE_CONFIG_VERSION']
        asset_time = remote_config['asset_digest_timestamp_ms'] / 1000000
        template_time = remote_config['item_templates_timestamp_ms'] / 1000

        download_settings = {}
        download_settings['hash'] = responses['DOWNLOAD_SETTINGS']['hash']
        download_settings['asset_time'] = asset_time
        download_settings['template_time'] = template_time

        account['remote_config'] = download_settings

        log.debug('Download settings for account %s: %s',
                  account['username'], download_settings)
        return True

    except Exception as e:
        log.error('Exception parsing download settings: %s.', repr(e))

    return False


def parse_level_up_rewards(api, account, responses):
    try:
        result = responses['LEVEL_UP_REWARDS'].get('result', 0)
        if result == 1:
            log.debug('Account %s collected its level up rewards.',
                      account['username'])
            # Parse item rewards into account inventory.
            parse_inventory(api, account, responses)
            return True
        elif result == 2:
            log.debug('Account %s already collected its level up rewards.',
                      account['username'])
            return True
    except Exception as e:
        log.error('Exception parsing level up rewards: %s.', repr(e))

    return False


# Parse player stats and inventory into account.
def parse_inventory(api, account, responses):
    try:
        inventory = responses['GET_INVENTORY']
        player_level = account['level']
        parsed_items = 0
        parsed_pokemons = 0
        parsed_eggs = 0
        for item in inventory['inventory_delta'].get('inventory_items', {}):
            item_data = item.get('inventory_item_data', {})
            if 'player_stats' in item_data:
                stats = item_data['player_stats']
                account['level'] = stats['level']
                account['experience'] = stats.get('experience', 0)
                account['encounters'] = stats.get('pokemons_encountered', 0)
                account['throws'] = stats.get('pokeballs_thrown', 0)
                account['catches'] = stats.get('pokemons_captured', 0)
                account['spins'] = stats.get('poke_stop_visits', 0)
                account['walked'] = stats.get('km_walked', 0)

                log.debug('Parsed %s player stats: level %d, %d XP, %f km ' +
                          'walked, %d encounters, %d catches and %d spins.',
                          account['username'], account['level'],
                          account['experience'], account['walked'],
                          account['encounters'], account['catches'],
                          account['spins'])
            elif 'item' in item_data:
                item_id = item_data['item']['item_id']
                item_count = item_data['item'].get('count', 0)
                account['items'][item_id] = item_count
                parsed_items += item_count
            elif 'egg_incubators' in item_data:
                incubators = item_data['egg_incubators']['egg_incubator']
                for incubator in incubators:
                    account['incubators'][incubator['id']] = {
                        'item_id': incubator['item_id'],
                        'uses_remaining': incubator.get('uses_remaining', 0),
                        'pokemon_id': incubator.get('pokemon_id', 0),
                        'km_walked': incubator.get('target_km_walked', 0)
                    }
            if 'pokemon_data' in item_data:
                p_data = item_data['pokemon_data']
                p_id = p_data.get('id', 0)
                pokemon_id = p_data.get('pokemon_id', 0)
                is_egg = p_data.get('is_egg', False)
                if p_id and pokemon_id:
                    account['pokemons'][p_id] = {
                        'pokemon_id': pokemon_id,
                        'move_1': p_data['move_1'],
                        'move_2': p_data['move_2'],
                        'height': p_data['height_m'],
                        'weight': p_data['weight_kg'],
                        'gender': p_data['pokemon_display']['gender'],
                        'cp': p_data['cp'],
                        'cp_multiplier': p_data['cp_multiplier']
                    }
                    parsed_pokemons += 1
                elif p_id and is_egg:
                    if p_data.get('egg_incubator_id', None):
                        # Egg is already incubating.
                        continue
                    account['eggs'][p_id] = {
                        'captured_cell_id': p_data['captured_cell_id'],
                        'creation_time_ms': p_data['creation_time_ms'],
                        'km_target': p_data['egg_km_walked_target']
                    }
                    parsed_eggs += 1
        log.debug(
            'Parsed %s player inventory: %d items, %d pokemons and %d eggs.',
            account['username'], parsed_items, parsed_pokemons, parsed_eggs)

        # Check if account has leveled up.
        if player_level > 0 and player_level < account['level']:
            log.info('Account %s has leveled up! Current level: %d',
                     account['username'], account['level'])
            time.sleep(random.uniform(1.7, 2.5))
            responses = request_level_up_rewards(api, account)
            if not parse_level_up_rewards(api, account, responses):
                log.warning('Account %s failed to collect level up rewards.',
                            account['username'])

        return True

    except Exception as e:
        log.error('Exception parsing player inventory: %s.', repr(e))

    return False


# Parse inventory for Egg Incubators.
def parse_use_item_egg_incubator(account, responses):
    try:
        use_egg_incubator = responses['USE_ITEM_EGG_INCUBATOR']
        result = use_egg_incubator.get('result', 0)
        if result != 1:
            log.error('Use egg incubator returned result code: %s', result)
            return False

        incubator = use_egg_incubator['egg_incubator']
        account['incubators'][incubator['id']] = {
            'item_id': incubator['item_id'],
            'uses_remaining': incubator.get('uses_remaining', 0),
            'pokemon_id': incubator.get('pokemon_id', 0),
            'km_walked': incubator.get('target_km_walked', 0)
        }
        return True

    except Exception as e:
        log.error('Exception parsing egg incubator: %s.', repr(e))

    return False


def incubate_eggs(api, account):
    incubators = dict(account['incubators'])
    for incubator_id, incubator in incubators.iteritems():
        egg_ids = account['eggs'].keys()
        if not egg_ids:
            log.debug('Account %s has no eggs to incubate.',
                      account['username'])
            break
        if incubator['pokemon_id'] == 0:
            egg_id = random.choice(egg_ids)
            km_target = account['eggs'][egg_id]['km_target']

            time.sleep(random.uniform(2.0, 4.0))
            responses = request_use_item_egg_incubator(api, account,
                                                       incubator_id, egg_id)
            if parse_use_item_egg_incubator(account, responses):
                message = (
                    'Egg #{} ({:.1f} km) is on incubator #{}.').format(
                    egg_id, km_target, incubator_id)
                log.info(message)
                del account['eggs'][egg_id]
            else:
                message = ('Failed to put egg #{} ({:.1f} km) on ' +
                           'incubator #{}.').format(
                    egg_id, km_target, incubator_id)
                log.error(message)
                return False

    return True


# https://docs.pogodev.org/api/enums/Item/
def recycle_items(status, api, account):
    item_ids = [1, 2, 3,
                101, 102, 103, 104, 201, 202,
                701, 703, 705]
    item_names = ['Pokeball', 'Greatball', 'Ultraball',
                  'Potion', 'Super Potion', 'Hyper Potion', 'Max Potion',
                  'Revive', 'Max Revive',
                  'Razz Berry', 'Nanab Berry', 'Pinap Berry']
    item_mins = [100, 40, 40,
                 10, 10, 10, 40, 10, 40,
                 10, 10, 10]
    item_ratios = [0.05, 0.05, 0.03,
                   0.30, 0.20, 0.20, 0.05, 0.10, 0.03,
                   0.20, 0.20, 0.20]
    indexes = range(len(item_ids))
    random.shuffle(indexes)
    for i in indexes:
        item_count = account['items'].get(item_ids[i], 0)
        if item_count > item_mins[i]:
            item_id = item_ids[i]
            item_name = item_names[i]
            drop_count = int(item_count * item_ratios[i])

            time.sleep(random.uniform(3.0, 5.0))
            responses = request_recycle_item(api, account, item_id, drop_count)
            recycle_item = responses['RECYCLE_INVENTORY_ITEM']
            if recycle_item.get('result', 0) > 0:
                account['items'][item_id] = recycle_item['new_count']
                status['message'] = 'Dropped items: {} {}.'.format(
                    drop_count, item_name)
                log.info(status['message'])
            else:
                status['message'] = 'Failed to recycle {} (id {}).'.format(
                    item_name, item_id)
                log.warning(status['message'])
                return False

    return True


# TODO: Jitter player location
def handle_pokestop(status, api, account, pokestop):
    pokestop_id = pokestop['id']
    location = account['last_location']

    if pokestop_id in account['used_pokestops']:
        return False
    if not recycle_items(status, api, account):
        return False

    time.sleep(random.uniform(2, 3))
    responses = request_fort_details(api, account, pokestop)

    if not responses.get('FORT_DETAILS', {}):
        status['message'] = (
            'Account {} failed to fetch Pokestop #{} details.').format(
                account['username'], pokestop_id)
        log.error(status['message'])
        return False

    status['message'] = 'Spinning Pokestop #{}.'.format(pokestop_id)
    log.info(status['message'])

    time.sleep(random.uniform(1.1, 2))
    responses = request_fort_search(api, account, pokestop, location)
    fort_search = responses.get('FORT_SEARCH', {})
    result = fort_search.get('result', 0)
    if result != 1:
        status['message'] = (
            'Account {} failed to spin Pokestop with result code: {}').format(
                account['username'], result)
        log.error(status['message'])
        return False

    if parse_inventory(api, account, responses):
        xp_awarded = fort_search.get('experience_awarded', 0)
        status['message'] = (
            'Account {} spun Pokestop and received {} XP.').format(
                account['username'], xp_awarded)
        log.info(status['message'])

        account['session_spins'] += 1
        account['session_experience'] += xp_awarded
        account['used_pokestops'][pokestop_id] = time.time()
        return True

    return False


def select_pokeball(account):
    item_ids = [1, 2, 3]
    item_names = ['Pokeball', 'Greatball', 'Ultraball']

    for i in range(3):
        if account['items'].get(item_ids[i], 0) > 0:
            return {'id': item_ids[i], 'name': item_names[i]}

    return False


def select_berry(account, berry=0.25):
    item_ids = [701, 703, 705]
    item_names = ['Razz Berry', 'Nanab Berry', 'Pinap Berry']

    if random.random() > berry:
        return False

    berries = []
    for i in range(3):
        if account['items'].get(item_ids[i], 0) > 0:
            berries.append({'id': item_ids[i], 'name': item_names[i]})

    if berries:
        return random.choice(berries)
    return False


# https://github.com/PokemonGoF/PokemonGo-Bot/blob/master/pokemongo_bot/cell_workers/pokemon_catch_worker.py
# Perfect Throw:
# normalized_reticle_size=1.950
# normalized_hit_position=1.0
# spin_modifier=1.0
def randomize_throw(excellent=0.05, great=0.5, nice=0.3, curveball=0.8):
    random_throw = random.random()
    great += excellent
    nice += great

    throw = {}
    if random_throw <= excellent:
        throw['name'] = 'Excellent'
        throw['reticle_size'] = 1.70 + 0.25 * random.random()
        throw['hit_position'] = 1.0
    elif random_throw <= great:
        throw['name'] = 'Great'
        throw['reticle_size'] = 1.30 + 0.399 * random.random()
        throw['hit_position'] = 1.0
    elif random_throw <= nice:
        throw['name'] = 'Nice'
        throw['reticle_size'] = 1.00 + 0.299 * random.random()
        throw['hit_position'] = 1.0
    else:
        # Not a any kind of special throw, let's throw a normal one.
        # Here the reticle size doesn't matter, we scored out of it.
        throw['name'] = 'Normal'
        throw['reticle_size'] = 1.25 + 0.70 * random.random()
        throw['hit_position'] = 0.0

    if curveball < random.random():
        throw['spin_modifier'] = 0.499 * random.random()
    else:
        throw['name'] += ' Curveball'
        throw['spin_modifier'] = 0.55 + 0.45 * random.random()

    if random.random() < 0.94:
        throw['hit_pokemon'] = 1
    else:
        throw['hit_pokemon'] = 0

    return throw


def catch_pokemon(status, api, account, pokemon, iv):
    pokemon_id = pokemon['pokemon_data']['pokemon_id']
    encounter_id = pokemon['encounter_id']
    spawnpoint_id = pokemon['spawn_point_id']

    attempts = 0
    max_attempts = random.randint(3, 5)
    used_berry = False
    while attempts < max_attempts:
        # Select Pokeball type to throw.
        ball = select_pokeball(account)
        if not ball:
            status['message'] = 'Account {} has no Pokeballs to throw.'.format(
                account['username'])
            log.warning(status['message'])
            return False

        if not used_berry:
            # Select a Berry type to use.
            berry = select_berry(account)
            if not berry:
                status['message'] = 'Account {} has no berries to use.'.format(
                    account['username'])
                log.info(status['message'])
            else:
                status['message'] = (
                    'Using a {} to catch Pokemon #{} - attempt {}.').format(
                        berry['name'], pokemon_id, attempts)
                log.info(status['message'])

                time.sleep(random.uniform(2, 4))

                responses = request_use_item_encounter(
                    api, account, encounter_id, spawnpoint_id, berry['id'])

                use_item = responses.get('USE_ITEM_ENCOUNTER', {})

                if use_item.get('active_item', 0) == berry['id']:
                    account['items'][berry['id']] -= 1
                    status['message'] = (
                        'Used a {} in encounter #{}.').format(
                            berry['name'], encounter_id)
                    log.debug(status['message'])
                else:
                    status['message'] = (
                        'Unable to use {} in encounter #{}.').format(
                            berry['name'], encounter_id)
                    log.error(status['message'])

        # Randomize throw.
        throw = randomize_throw()

        status['message'] = (
            'Catching Pokemon #{} - {} throw using {} - attempt {}.').format(
                pokemon_id, throw['name'], ball['name'], attempts)
        log.info(status['message'])

        time.sleep(random.uniform(3, 5))
        responses = request_catch_pokemon(api, account, encounter_id,
                                          spawnpoint_id, throw, ball['id'])
        account['session_throws'] += 1

        catch_pokemon = responses.get('CATCH_POKEMON', {})
        catch_status = catch_pokemon.get('status', -1)
        if catch_status <= 0:
            status['message'] = (
                'Account {} failed to catch Pokemon #{}: {}').format(
                    account['username'], pokemon_id, catch_status)
            log.error(status['message'])
            return False
        if catch_status == 1:
            catch_id = catch_pokemon['captured_pokemon_id']
            xp_awarded = sum(catch_pokemon['capture_award']['xp'])

            status['message'] = (
                'Caught Pokemon #{} {} with {} and received {} XP').format(
                    pokemon_id, catch_id, ball['name'], xp_awarded)
            log.info(status['message'])

            account['session_catches'] += 1
            account['session_experience'] += xp_awarded

            # Check if caught Pokemon is a Ditto.
            # Parse Pokemons in response and update account inventory.
            parse_inventory(api, account, responses)

            caught_pokemon = account['pokemons'].get(catch_id, None)
            if not caught_pokemon:
                log.error('Pokemon %s not found in inventory.', catch_id)
                return False

            # Don't release all Pokemon.
            keep_pokemon = random.random()
            if (iv > 80 and keep_pokemon < 0.65) or (
                    iv > 91 and keep_pokemon < 0.95):
                log.info('Kept Pokemon #%d (IV %d) in inventory (%d/%d).',
                         pokemon_id, iv,
                         len(account['pokemons']), account['max_pokemons'])
                return caught_pokemon

            release_pokemon(status, api, account, catch_id)
            return caught_pokemon

        if catch_status == 2:
            status['message'] = (
                'Catch attempt {} failed. Pokemon #{} broke free.').format(
                    attempts, pokemon_id)
            log.info(status['message'])
            used_berry = False
        if catch_status == 3:
            status['message'] = (
                'Catch attempt {} failed. Pokemon #{} fled!').format(
                    attempts, pokemon_id)
            log.info(status['message'])
            break
        if catch_status == 4:
            status['message'] = (
                'Catch attempt {} failed. Pokemon #{} dodged.').format(
                    attempts, pokemon_id)
            log.info(status['message'])
            if berry:
                # Prevent attempts to use a berry again if one still active
                used_berry = True

        attempts += 1
    return False


def release_pokemon(status, api, account, catch_id):
    total_pokemons = len(account['pokemons'])
    max_pokemons = account['max_pokemons']

    log.debug('Account %s inventory has %d / %d Pokemons.',
              account['username'], total_pokemons, max_pokemons)

    time.sleep(random.uniform(4, 6))

    release_ids = []
    if total_pokemons > max_pokemons * 0.9:
        release_count = int(total_pokemons * 0.03)  # should be around 9
        release_ids = random.sample(account['pokemons'].keys(), release_count)
        release_ids.append(catch_id)
        responses = request_release_pokemon(api, account, 0, release_ids)
    else:
        release_ids.append(catch_id)
        responses = request_release_pokemon(api, account, catch_id)

    release_result = responses.get('RELEASE_POKEMON', {}).get('result', 0)

    if release_result == 1:
        status['message'] = 'Released Pokemon: {}'.format(release_ids)
        log.info(status['message'])

        for p_id in release_ids:
            account['pokemons'].pop(p_id, None)
        return True
    else:
        status['message'] = 'Failed to release Pokemon: {}'.format(release_ids)
        log.warning(status['message'])
        return False


# https://docs.pogodev.org/api/messages/GetPlayerProto/
# https://docs.pogodev.org/api/messages/GetPlayerOutProto/
def request_get_player(api, account, login=False, buddy=True):
    try:
        req = api.create_request()
        response = req.get_player(
            player_locale={
                'country': 'US',
                'language': 'en',
                'timezone': 'America/Los_Angeles'})
        if not login:
            req.check_challenge()
            req.get_hatched_eggs()
            req.get_inventory(last_timestamp_ms=account['last_timestamp_ms'])
            req.check_awarded_badges()
            if buddy:
                req.get_buddy_walked()
        response = req.call()

        if not login:
            account['last_timestamp_ms'] = parse_new_timestamp_ms(response)
        return response['responses']

    except Exception as e:
        log.error('Exception getting player information: %s', repr(e))

    return False


# https://docs.pogodev.org/api/messages/FortDetailsProto/
# https://docs.pogodev.org/api/messages/FortDetailsOutProto/
def request_fort_details(api, account, pokestop):
    try:
        req = api.create_request()
        response = req.fort_details(
            fort_id=pokestop['id'],
            latitude=pokestop['latitude'],
            longitude=pokestop['longitude'])
        req.check_challenge()
        req.get_hatched_eggs()
        req.get_inventory(last_timestamp_ms=account['last_timestamp_ms'])
        req.check_awarded_badges()
        # req.download_settings(hash=account['remote_config']['hash'])
        req.get_buddy_walked()
        response = req.call()

        account['last_timestamp_ms'] = parse_new_timestamp_ms(response)
        return response['responses']

    except Exception as e:
        log.error('Exception while fetching Pokestop details: %s.', repr(e))

    return False


# https://docs.pogodev.org/api/messages/FortSearchProto/
# https://docs.pogodev.org/api/messages/FortSearchOutProto
def request_fort_search(api, account, pokestop, location):
    try:
        req = api.create_request()
        response = req.fort_search(
            fort_id=pokestop['id'],
            fort_latitude=pokestop['latitude'],
            fort_longitude=pokestop['longitude'],
            player_latitude=location[0],
            player_longitude=location[1])
        req.check_challenge()
        req.get_hatched_eggs()
        req.get_inventory(last_timestamp_ms=account['last_timestamp_ms'])
        req.check_awarded_badges()
        # req.download_settings(hash=account['remote_config']['hash'])
        req.get_buddy_walked()
        response = req.call()

        account['last_timestamp_ms'] = parse_new_timestamp_ms(response)
        return response['responses']

    except Exception as e:
        log.error('Exception while searching Pokestop: %s.', repr(e))

    return False


def request_encounter(api, account, encounter_id, spawnpoint_id, location):
    try:
        # Setup encounter request envelope.
        req = api.create_request()
        response = req.encounter(
            encounter_id=encounter_id,
            spawn_point_id=spawnpoint_id,
            player_latitude=location[0],
            player_longitude=location[1])
        req.check_challenge()
        req.get_hatched_eggs()
        req.get_inventory(last_timestamp_ms=account['last_timestamp_ms'])
        req.check_awarded_badges()
        # req.download_settings(hash=account['remote_config']['hash'])
        req.get_buddy_walked()
        response = req.call()

        account['last_timestamp_ms'] = parse_new_timestamp_ms(response)
        return response

    except Exception as e:
        log.error('Exception while encountering Pokemon: %s.', repr(e))

    return False


# https://docs.pogodev.org/api/messages/RecycleItemProto/
# https://docs.pogodev.org/api/messages/RecycleItemOutProto
def request_recycle_item(api, account, item_id, amount):
    try:
        req = api.create_request()
        response = req.recycle_inventory_item(item_id=item_id, count=amount)
        req.check_challenge()
        req.get_hatched_eggs()
        req.get_inventory(last_timestamp_ms=account['last_timestamp_ms'])
        req.check_awarded_badges()
        # req.download_settings(hash=account['remote_config']['hash'])
        req.get_buddy_walked()
        response = req.call()

        account['last_timestamp_ms'] = parse_new_timestamp_ms(response)
        return response['responses']

    except Exception as e:
        log.warning('Exception while dropping items: %s', repr(e))

    return False


# https://docs.pogodev.org/api/messages/GetRemoteConfigVersionsProto/
# https://docs.pogodev.org/api/messages/GetRemoteConfigVersionsOutProto/
def request_download_settings(api, account, app_version):
    try:
        req = api.create_request()
        response = req.download_remote_config_version(
            platform=1,
            # device_manufacturer=account['device_info']['device_brand'],
            # device_model=account['device_info']['device_model'],
            # locale='en_US',
            app_version=app_version)
        req.check_challenge()
        req.get_hatched_eggs()
        req.get_inventory(last_timestamp_ms=0)
        req.check_awarded_badges()
        req.download_settings()
        response = req.call()

        account['last_timestamp_ms'] = parse_new_timestamp_ms(response)
        return response

    except Exception as e:
        log.error('Exception while downloading app settings: %s.', repr(e))

    return False


def request_get_asset_digest(api, account, app_version, offset, timestamp):
    try:
        req = api.create_request()
        response = req.get_asset_digest(
            platform=1,
            app_version=app_version,
            paginate=True,
            page_offset=offset,
            page_timestamp=timestamp)
        req.check_challenge()
        req.get_hatched_eggs()
        req.get_inventory(last_timestamp_ms=account['last_timestamp_ms'])
        req.check_awarded_badges()
        req.download_settings(hash=account['remote_config']['hash'])
        response = req.call()

        account['last_timestamp_ms'] = parse_new_timestamp_ms(response)
        return response['responses']

    except Exception as e:
        log.error('Exception while getting asset digest: %s.', repr(e))

    return False


def request_download_item_templates(api, account, offset, timestamp):
    try:
        req = api.create_request()
        response = req.download_item_templates(
            paginate=True,
            page_offset=offset,
            page_timestamp=timestamp)
        req.check_challenge()
        req.get_hatched_eggs()
        req.get_inventory(last_timestamp_ms=account['last_timestamp_ms'])
        req.check_awarded_badges()
        req.download_settings(hash=account['remote_config']['hash'])
        response = req.call()

        account['last_timestamp_ms'] = parse_new_timestamp_ms(response)
        return response['responses']

    except Exception as e:
        log.error('Exception while downloading item templates: %s.', repr(e))

    return False


def request_get_player_profile(api, account, login=False):
    try:
        req = api.create_request()
        req.get_player_profile()
        req.check_challenge()
        req.get_hatched_eggs()
        req.get_inventory(last_timestamp_ms=account['last_timestamp_ms'])
        req.check_awarded_badges()
        if login:
            req.download_settings(hash=account['remote_config']['hash'])
        req.get_buddy_walked()
        response = req.call()

        account['last_timestamp_ms'] = parse_new_timestamp_ms(response)
        return response['responses']

    except Exception as e:
        log.warning('Exception while requesting player profile: %s', repr(e))

    return False


# https://docs.pogodev.org/api/messages/LevelUpRewardsProto/
# https://docs.pogodev.org/api/messages/LevelUpRewardsOutProto/
def request_level_up_rewards(api, account, login=False):
    try:
        req = api.create_request()
        response = req.level_up_rewards(level=account['level'])
        req.check_challenge()
        req.get_hatched_eggs()
        req.get_inventory(last_timestamp_ms=account['last_timestamp_ms'])
        req.check_awarded_badges()
        if login:
            req.download_settings(hash=account['remote_config']['hash'])
        req.get_buddy_walked()
        response = req.call()

        account['last_timestamp_ms'] = parse_new_timestamp_ms(response)
        return response['responses']

    except Exception as e:
        log.warning('Exception while requesting level up rewards: %s', repr(e))

    return False


def request_mark_tutorial_complete(api, account, tutorial):
    try:
        req = api.create_request()
        response = req.mark_tutorial_complete(tutorials_completed=tutorial)
        req.check_challenge()
        req.get_hatched_eggs()
        req.get_inventory(last_timestamp_ms=account['last_timestamp_ms'])
        req.check_awarded_badges()
        response = req.call()

        account['last_timestamp_ms'] = parse_new_timestamp_ms(response)
        return response['responses']

    except Exception as e:
        log.warning('Exception while marking tutorial complete: %s', repr(e))

    return False


# https://docs.pogodev.org/api/messages/CatchPokemonProto/
# https://docs.pogodev.org/api/messages/CatchPokemonOutProto/
def request_catch_pokemon(api, account, encounter_id, spawnpoint_id, throw,
                          ball_id=1):
    try:
        req = api.create_request()
        response = req.catch_pokemon(
            encounter_id=encounter_id,
            pokeball=ball_id,
            normalized_reticle_size=throw['reticle_size'],
            spawn_point_id=spawnpoint_id,
            hit_pokemon=throw['hit_pokemon'],
            spin_modifier=throw['spin_modifier'],
            normalized_hit_position=throw['hit_position'])
        req.check_challenge()
        req.get_hatched_eggs()
        req.get_inventory(last_timestamp_ms=account['last_timestamp_ms'])
        req.check_awarded_badges()
        # req.download_settings(hash=account['remote_config']['hash'])
        req.get_buddy_walked()
        response = req.call()

        account['last_timestamp_ms'] = parse_new_timestamp_ms(response)
        return response['responses']

    except Exception as e:
        log.warning('Exception while catching Pokemon: %s', repr(e))

    return False


# https://docs.pogodev.org/api/messages/UseItemCaptureProto/
# https://docs.pogodev.org/api/messages/UseItemCaptureOutProto/
def request_use_item_encounter(api, account, encounter_id, spawnpoint_id,
                               berry_id=701):
    try:
        req = api.create_request()
        response = req.use_item_encounter(
            item=berry_id,
            encounter_id=encounter_id,
            spawn_point_guid=spawnpoint_id)
        req.check_challenge()
        req.get_hatched_eggs()
        req.get_inventory(last_timestamp_ms=account['last_timestamp_ms'])
        req.check_awarded_badges()
        # req.download_settings(hash=account['remote_config']['hash'])
        req.get_buddy_walked()
        response = req.call()

        account['last_timestamp_ms'] = parse_new_timestamp_ms(response)
        return response['responses']

    except Exception as e:
        log.warning('Exception while using a Berry on a Pokemon: %s', repr(e))

    return False


# https://docs.pogodev.org/api/messages/ReleasePokemonProto
# https://docs.pogodev.org/api/messages/ReleasePokemonOutProto/
def request_release_pokemon(api, account, pokemon_id, release_ids=[]):
    try:
        req = api.create_request()
        response = req.release_pokemon(
            pokemon_id=pokemon_id,
            pokemon_ids=release_ids
        )
        req.check_challenge()
        req.get_hatched_eggs()
        req.get_inventory(last_timestamp_ms=account['last_timestamp_ms'])
        req.check_awarded_badges()
        # req.download_settings(hash=account['remote_config']['hash'])
        req.get_buddy_walked()
        response = req.call()

        account['last_timestamp_ms'] = parse_new_timestamp_ms(response)
        return response['responses']

    except Exception as e:
        log.error('Exception while releasing Pokemon: %s', repr(e))

    return False


# https://docs.pogodev.org/api/messages/UseItemEggIncubatorProto/
# https://docs.pogodev.org/api/messages/UseItemEggIncubatorOutProto/
def request_use_item_egg_incubator(api, account, incubator_id, egg_id):
    try:
        req = api.create_request()
        response = req.use_item_egg_incubator(
            item_id=incubator_id,
            pokemon_id=egg_id
        )
        req.check_challenge()
        req.get_hatched_eggs()
        req.get_inventory(last_timestamp_ms=account['last_timestamp_ms'])
        req.check_awarded_badges()
        # req.download_settings(hash=account['remote_config']['hash'])
        req.get_buddy_walked()
        response = req.call()

        account['last_timestamp_ms'] = parse_new_timestamp_ms(response)
        return response['responses']

    except Exception as e:
        log.warning('Exception while putting an egg in incubator: %s', repr(e))

    return False


# The AccountSet returns a scheduler that cycles through different
# sets of accounts (e.g. L30). Each set is defined at runtime, and is
# (currently) used to separate regular accounts from L30 accounts.
# TODO: Migrate the old account Queue to a real AccountScheduler, preferably
# handled globally via database instead of per instance.
# TODO: Accounts in the AccountSet are exempt from things like the
# account recycler thread. We could've hardcoded support into it, but that
# would have added to the amount of ugly code. Instead, we keep it as is
# until we have a proper account manager.
class AccountSet(object):

    def __init__(self, kph):
        self.sets = {}

        # Scanning limits.
        self.kph = kph

        # Thread safety.
        self.next_lock = Lock()

    # Set manipulation.
    def create_set(self, name, values=[]):
        if name in self.sets:
            raise Exception('Account set ' + name + ' is being created twice.')

        self.sets[name] = values

    # Release an account back to the pool after it was used.
    def release(self, account):
        if 'in_use' not in account:
            log.error('Released account %s back to the AccountSet,'
                      + " but it wasn't locked.",
                      account['username'])
        else:
            account['in_use'] = False

    # Get next account that is ready to be used for scanning.
    def next(self, set_name, coords_to_scan):
        # Yay for thread safety.
        with self.next_lock:
            # Readability.
            account_set = self.sets[set_name]

            # Loop all accounts for a good one.
            now = default_timer()
            max_speed_kmph = self.kph

            for i in range(len(account_set)):
                account = account_set[i]

                # Make sure it's not in use.
                if account.get('in_use', False):
                    continue

                # Make sure the account hasn't failed.
                if account.get('failed', False):
                    continue

                # Check if we're below speed limit for account.
                last_scanned = account.get('last_scanned', False)

                if last_scanned:
                    seconds_passed = now - last_scanned
                    old_coords = account.get('last_coords', coords_to_scan)

                    distance_km = equi_rect_distance(
                        old_coords,
                        coords_to_scan)
                    cooldown_time_sec = distance_km / max_speed_kmph * 3600

                    # Not enough time has passed for this one.
                    if seconds_passed < cooldown_time_sec:
                        continue

                # We've found an account that's ready.
                account['last_scanned'] = now
                account['last_coords'] = coords_to_scan
                account['in_use'] = True

                return account

        # TODO: Instead of returning False, return the amount of min. seconds
        # the instance needs to wait until the first account becomes available,
        # so it doesn't need to keep asking if we know we need to wait.
        return False
