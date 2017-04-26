#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import time
import random

from pgoapi.exceptions import AuthException, BannedAccountException

log = logging.getLogger(__name__)


class TooManyLoginAttempts(Exception):
    pass


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
            log.error('Account %s is banned from Pokemon Go.',
                      account['username'])
            time.sleep(args.login_delay)
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

    log.debug('Login for account %s successful.', account['username'])
    time.sleep(20)


# Check if player has received any warnings or is banned.
# Check if all important tutorial steps have been completed.
def get_player_state(api, account):
    try:
        time.sleep(random.uniform(1, 2))
        req = api.create_request()
        req.get_player(
            player_locale={
                'country': 'US',
                'language': 'en',
                'timezone': 'America/Los_Angeles'})
        res = req.check_challenge()
        res = req.call()

        get_player = res.get('responses', {}).get('GET_PLAYER', {})
        warning_state = get_player.get('warn', None)
        banned_state = get_player.get('banned', False)
        player_data = get_player.get('player_data', {})
        tutorial_state = player_data.get('tutorial_state', [])
        max_items = player_data.get('max_item_storage', 350)
        max_pokemons = player_data.get('max_pokemon_storage', 250)

        account['warning'] = warning_state
        account['banned'] = banned_state
        account['tutorials'] = tutorial_state
        account['max_items'] = max_items
        account['max_pokemons'] = max_pokemons

        return True
    except Exception as e:
        log.warning('Exception while getting player state: %s', repr(e))

    return False


# Complete minimal tutorial steps.
# API argument needs to be a logged in API instance.
# TODO: Check if game client bundles these requests, or does them separately.
def complete_tutorial(api, account):
    tutorial_state = account['tutorials']
    if 0 not in tutorial_state:
        time.sleep(random.uniform(1, 5))
        request = api.create_request()
        request.mark_tutorial_complete(tutorials_completed=0)
        log.debug('Sending 0 tutorials_completed for %s.', account['username'])
        request.call()

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

        request = api.create_request()
        request.mark_tutorial_complete(tutorials_completed=1)
        log.debug('Sending 1 tutorials_completed for %s.', account['username'])
        request.call()

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
        request = api.create_request()
        request.get_player(
            player_locale={
                'country': 'US',
                'language': 'en',
                'timezone': 'America/Denver'})
        responses = request.call().get('responses', {})

        inventory = responses.get('GET_INVENTORY', {}).get(
            'inventory_delta', {}).get('inventory_items', [])
        for item in inventory:
            pokemon = item.get('inventory_item_data', {}).get('pokemon_data')
            if pokemon:
                starter_id = pokemon.get('id')

    if 4 not in tutorial_state:
        time.sleep(random.uniform(5, 12))
        request = api.create_request()
        request.claim_codename(codename=account['username'])
        log.debug('Claiming codename for %s.', account['username'])
        request.call()

        time.sleep(random.uniform(1, 1.3))
        request = api.create_request()
        request.mark_tutorial_complete(tutorials_completed=4)
        log.debug('Sending 4 tutorials_completed for %s.', account['username'])
        request.call()

        time.sleep(0.1)
        request = api.create_request()
        request.get_player(
            player_locale={
                'country': 'US',
                'language': 'en',
                'timezone': 'America/Denver'})
        request.call()

    if 7 not in tutorial_state:
        time.sleep(random.uniform(4, 10))
        request = api.create_request()
        request.mark_tutorial_complete(tutorials_completed=7)
        log.debug('Sending 7 tutorials_completed for %s.', account['username'])
        request.call()

    if starter_id:
        time.sleep(random.uniform(3, 5))
        request = api.create_request()
        request.set_buddy_pokemon(pokemon_id=starter_id)
        log.debug('Setting buddy pokemon for %s.', account['username'])
        request.call()
        time.sleep(random.uniform(0.8, 1.8))

    # Sleeping before we start scanning to avoid Niantic throttling.
    log.debug('And %s is done. Wait for a second, to avoid throttle.',
              account['username'])
    time.sleep(random.uniform(2, 4))
    return True


def cleanup_account_stats(account, pokestop_timeout):
    # Do hourly account statistics cleanup.
    last_cleanup = account['last_cleanup']
    if (last_cleanup + 3600) < time.time():
        log.info('Account %s hourly stats: %d throws - %d captures - %d spins',
                 account['username'], account['hour_throws'],
                 account['hour_captures'], account['hour_spins'])
        # These counters are used to limit levelling actions per hour.
        account['hour_throws'] = 0
        account['hour_captures'] = 0
        account['hour_spins'] = 0
        account['last_cleanup'] = time.time()

    # Refresh visited pokestops that were on timeout.
    used_pokestops = dict(account['used_pokestops'])
    for pokestop_id in account['used_pokestops']:
        last_attempt = account['used_pokestops'][pokestop_id]
        if (last_attempt + pokestop_timeout) < time.time():
            del used_pokestops[pokestop_id]
    account['used_pokestops'] = used_pokestops


# Parse player level and inventory into account dictionary.
# Manage account statistics and does regular cleanup.
def parse_account_stats(args, api, response_dict, account):
    if account['first_login']:
        # Check if account is banned.
        status_code = response_dict.get('status_code', -1)
        if status_code == 3:
            account['banned'] = True
            log.warning('Account %s is probably banned.', account['username'])

        # Check if there are level up rewards to claim.
        time.sleep(random.uniform(2.0, 3.0))
        if request_level_up_rewards(api, account):
            log.info('Account %s collected its level up rewards.',
                     account['username'])
        else:
            log.info('Account %s already collected level up rewards.',
                     account['username'])

    cleanup_account_stats(account, args.pokestop_refresh_time)

    # Parse inventory items into account.
    inventory_items = response_dict['responses'].get(
        'GET_INVENTORY', {}).get(
        'inventory_delta', {}).get(
        'inventory_items', [])
    player_stats = {}
    player_items = {}
    total_items = 0
    for item in inventory_items:
        item_data = item.get('inventory_item_data', {})
        if 'player_stats' in item_data:
            player_stats = item_data['player_stats']
        elif 'item' in item_data:
            item_id = item_data['item'].get('item_id', 0)
            item_count = item_data['item'].get('count', 0)
            player_items[item_id] = item_count
            total_items += item_count

    player_level = player_stats.get('level', 0)
    log.debug('Account %s is level %d and has %d items: %s',
              account['username'], player_level, total_items, player_items)

    if player_level > 0:
        if account['level'] > 0 and player_level > account['level']:
            log.info('Account %s has leveled up! Current level: %d',
                     account['username'], player_level)
            time.sleep(random.uniform(2.0, 3.0))
            if request_level_up_rewards(api, account):
                log.debug('Account %s collected its level up rewards.',
                          account['username'])
            else:
                log.warning('Account %s failed to collect level up rewards.',
                            account['username'])

        account['level'] = player_level
        account['items'] = player_items
        account['item_count'] = total_items
        account['experience'] = player_stats.get('experience', 0L)
        account['encounters'] = player_stats.get('pokemons_encountered', 0)
        account['throws'] = player_stats.get('pokeballs_thrown', 0)
        account['captures'] = player_stats.get('pokemons_captured', 0)
        account['spins'] = player_stats.get('poke_stop_visits', 0)
        account['walked'] = player_stats.get('km_walked', 0.0)

        return True

    return False


def parse_player_pokemons(response_dict):
    pokemons = {}
    # Check inventory for Pokemon data.
    inventory_items = response_dict['responses'].get(
        'GET_INVENTORY', {}).get(
        'inventory_delta', {}).get(
        'inventory_items', [])

    for item in inventory_items:
        if 'pokemon_data' in item['inventory_item_data']:
            p_data = item['inventory_item_data']['pokemon_data']
            p_id = p_data.get('id', 0L)
            if p_id:
                pokemons[p_id] = {
                    'pokemon_id': p_data['pokemon_id'],
                    'move_1': p_data['move_1'],
                    'move_2': p_data['move_2'],
                    'height': p_data['height_m'],
                    'weight': p_data['weight_kg'],
                    'gender': p_data['pokemon_display']['gender'],
                    'cp': p_data['cp']
                }

    return pokemons


# https://docs.pogodev.org/api/enums/Item/
# ITEM_POKEBALL = 1
# ITEM_GREATBALL = 2
# ITEM_ULTRABALL = 3
# ITEM_POTION = 101
# ITEM_SUPER_POTION = 102
# ITEM_HYPER_POTION = 103
# ITEM_MAX_POTION = 104
# ITEM_REVIVE = 201
# ITEM_MAX_REVIVE = 202
# ITEM_RAZZ_BERRY = 701
# ITEM_NANAB_BERRY = 703
# ITEM_PINAP_BERRY = 705
def recycle_items(status, api, account):
    item_names = ['Pokeball', 'Greatball', 'Ultraball',
                  'Potion', 'Super Potion', 'Hyper Potion', 'Hyper Potion',
                  'Max Potion', 'Revive', 'Max Revive',
                  'Razz Berry', 'Nanab Berry', 'Pinap Berry']
    item_ids = [1, 2, 3,
                101, 102, 103, 104, 201, 202,
                701, 703, 705]
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

            status['message'] = 'Trying to drop {} {}.'.format(
                drop_count, item_name)
            log.info(status['message'])
            time.sleep(random.uniform(3.0, 5.0))
            new_count = request_recycle_item(api, item_id, drop_count)
            if new_count == -1:
                status['message'] = 'Failed to recycle {} (id {}).'.format(
                    item_name, item_id)
                log.warning(status['message'])
                return False
            account['items'][item_id] = new_count

    return True


def handle_pokestop(status, api, account, pokestop):
    pokestop_id = pokestop['id']
    location = account['last_location']

    if pokestop_id in account['used_pokestops']:
        return False
    if not recycle_items(status, api, account):
        return False

    attempts = 1
    while attempts < 4:
        status['message'] = 'Spinning Pokestop {} - attempt {}.'.format(
            pokestop_id, attempts)
        log.info(status['message'])
        time.sleep(random.uniform(2, 3))

        spin_response = request_fort_search(api, pokestop, location)
        account['hour_spins'] += 1

        # Check for captcha
        captcha_url = spin_response['responses'][
            'CHECK_CHALLENGE']['challenge_url']
        if len(captcha_url) > 1:
            status['message'] = 'Captcha encountered while spinning Pokestop.'
            log.info(status['message'])
            return False

        fort_search = spin_response['responses'].get('FORT_SEARCH', {})
        if fort_search:
            spin_result = fort_search.get('result', -1)
            spun_pokestop = True
            if spin_result == 1:
                xp_awarded = fort_search.get('experience_awarded', 0)
                status['message'] = ('Account {} (lvl {}) spun Pokestop and ' +
                                     'received {} XP.').format(
                    account['username'], account['level'], xp_awarded)
            elif spin_result == 2:
                log.warning('Pokestop out of range.')
            elif spin_result == 3:
                log.warning('Pokestop is on cooldown.')
            elif spin_result == 4:
                log.warning('Inventory is full.')
            elif spin_result == 5:
                log.warning('Pokestop daily quota reached.')
            else:
                log.warning('Unable to spin Pokestop, unknown return: %s',
                            spin_result)
                spun_pokestop = False

            if spun_pokestop:
                account['used_pokestops'][pokestop_id] = time.time()
                return True

        attempts += 1
    return False


# https://docs.pogodev.org/api/messages/CatchPokemonProto/
# https://docs.pogodev.org/api/messages/CatchPokemonOutProto/
def catch_pokemon(status, api, account, pokemon):
    pokemon_id = pokemon['pokemon_data']['pokemon_id']
    encounter_id = pokemon['encounter_id']
    spawnpoint_id = pokemon['spawn_point_id']

    # Try to catch pokemon, but don't get stuck.
    attempts = 1
    while attempts < 4:
        # Select Pokeball type to throw.
        if account['items'].get(1, 0) > 0:
            ball_id = 1
        elif account['items'].get(2, 0) > 0:
            ball_id = 2
        elif account['items'].get(3, 0) > 0:
            ball_id = 3
        else:
            status['message'] = (
                'Account {} has no Pokeballs to throw at Pokemon #{}.').format(
                    account['username'], pokemon_id)
            log.warning(status['message'])

            return False
        status['message'] = (
            'Catching Pokemon #{} using ball #{} - attempt {}.').format(
                pokemon_id, ball_id, attempts)
        log.info(status['message'])
        time.sleep(random.uniform(3, 5))

        res = request_catch_pokemon(api, encounter_id, spawnpoint_id, ball_id)
        account['hour_throws'] += 1

        catch_pokemon = res['responses'].get('CATCH_POKEMON', {})
        if catch_pokemon:

            catch_status = catch_pokemon.get('status', -1)

            if catch_status <= 0:
                status['message'] = (
                    'Account {} failed to catch Pokemon #{}: {}').format(
                        account['username'], pokemon_id, catch_status)
                log.warning(status['message'])
                return False
            if catch_status == 1:
                account['hour_captures'] += 1

                catch_id = catch_pokemon['captured_pokemon_id']
                status['message'] = (
                    'Caught Pokemon #{} {} with ball #{}!').format(
                        pokemon_id, catch_id, ball_id)
                log.info(status['message'])

                # Check if caught Pokemon is a Ditto.
                inventory_items = res['responses'].get(
                    'GET_INVENTORY', {}).get(
                    'inventory_delta', {}).get(
                    'inventory_items', [])

                pokemon_caught = {}
                for item in inventory_items:
                    if 'pokemon_data' in item['inventory_item_data']:
                        p_data = item['inventory_item_data']['pokemon_data']
                        p_id = p_data.get('id', 0L)
                        if catch_id == p_id:
                            # TODO: maybe we can update pokemon here.
                            pokemon_caught = {
                                'pokemon_id': p_data['pokemon_id'],
                                'move_1': p_data['move_1'],
                                'move_2': p_data['move_2'],
                                'height': p_data['height_m'],
                                'weight': p_data['weight_kg'],
                                'gender': p_data['pokemon_display']['gender'],
                                'cp': p_data['cp']
                            }

                time.sleep(random.uniform(4, 6))

                if request_release_pokemon(api, catch_id):
                    status['message'] = (
                        'Released Pokemon {} after capture.').format(
                            catch_id)
                    log.info(status['message'])
                else:
                    status['message'] = (
                        'Unable to release captured Pokemon {}.').format(
                            catch_id)
                    log.warning(status['message'])

                if not pokemon_caught:
                    log.error('Pokemon %s not found in inventory.', catch_id)
                    return False

                return pokemon_caught

            if catch_status == 2:
                status['message'] = (
                    'Catch attempt {} failed. Pokemon #{} broke free.').format(
                        attempts, pokemon_id)
                log.info(status['message'])
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

        attempts += 1
    return False


def request_fort_search(api, pokestop, location):
    try:
        req = api.create_request()
        res = req.fort_search(fort_id=pokestop['id'],
                              fort_latitude=pokestop['latitude'],
                              fort_longitude=pokestop['longitude'],
                              player_latitude=location[0],
                              player_longitude=location[1])
        res = req.check_challenge()
        res = req.get_hatched_eggs()
        res = req.get_inventory()
        res = req.check_awarded_badges()
        res = req.download_settings()
        res = req.get_buddy_walked()
        res = req.call()

        return res
    except Exception as e:
        log.warning('Exception while spinning Pokestop: %s', repr(e))

    return False


def request_recycle_item(api, item_id, amount):
    try:
        req = api.create_request()
        res = req.recycle_inventory_item(item_id=item_id, count=amount)
        res = req.check_challenge()   # real app behavior
        res = req.get_inventory()   # real app behavior
        res = req.call()

        recycle_item = res['responses'].get('RECYCLE_INVENTORY_ITEM', {})
        if recycle_item:
            drop_result = recycle_item.get('result', 0)
            if drop_result == 1:
                return recycle_item.get('new_count', 0)

    except Exception as e:
        log.warning('Exception while dropping items: %s', repr(e))

    return -1


# Send LevelUpRewards request to check for and accept level up rewards.
def request_level_up_rewards(api, account):
    try:
        req = api.create_request()
        res = req.level_up_rewards(level=account['level'])
        res = req.check_challenge()
        res = req.call()

        rewards = res['responses'].get('LEVEL_UP_REWARDS', {})

        if rewards.get('result', 0):
            return True

    except Exception as e:
        log.warning('Exception while requesting level up rewards: %s', repr(e))

    return False


def request_catch_pokemon(api, encounter_id, spawnpoint_id, ball_id=1):
    try:
        req = api.create_request()
        res = req.catch_pokemon(
            encounter_id=encounter_id,
            pokeball=ball_id,
            normalized_reticle_size=1.950,
            spawn_point_id=spawnpoint_id,
            hit_pokemon=1,
            spin_modifier=1.0,
            normalized_hit_position=1.0)
        res = req.check_challenge()
        res = req.get_hatched_eggs()
        res = req.get_inventory()
        res = req.check_awarded_badges()
        res = req.download_settings()
        res = req.get_buddy_walked()
        res = req.call()

        return res
    except Exception as e:
        log.warning('Exception while catching Pokemon: %s', repr(e))

    return False


def request_release_pokemon(api, pokemon_id):
    try:
        req = api.create_request()
        res = req.release_pokemon(pokemon_id=pokemon_id)
        res = req.check_challenge()
        res = req.get_inventory()
        res = req.call()

        release_pokemon = res['responses'].get('RELEASE_POKEMON', {})
        if release_pokemon:
            release_result = release_pokemon.get('result', 0)
            if release_result == 1:
                return True
            else:
                log.warning('Failed to release pokemon %s. Result code: %s.',
                            pokemon_id, release_result)
    except Exception as e:
        log.error('Exception while releasing Pokemon: %s', repr(e))

    return False
