#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import time
import random

from pgoapi.exceptions import AuthException

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


# XXX: unused
# Check if all important tutorial steps have been completed.
# API argument needs to be a logged in API instance.
def get_tutorial_state(api, account):
    log.debug('Checking tutorial state for %s.', account['username'])
    request = api.create_request()
    request.get_player(
        player_locale={'country': 'US',
                       'language': 'en',
                       'timezone': 'America/Denver'})

    response = request.call().get('responses', {})

    get_player = response.get('GET_PLAYER', {})
    tutorial_state = get_player.get(
        'player_data', {}).get('tutorial_state', [])
    time.sleep(random.uniform(2, 4))
    return tutorial_state


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


# Used by models.py::parse_map
def get_player_level(map_dict):
    inventory_items = map_dict['responses'].get(
        'GET_INVENTORY', {}).get(
        'inventory_delta', {}).get(
        'inventory_items', [])
    player_stats = [item['inventory_item_data']['player_stats']
                    for item in inventory_items
                    if 'player_stats' in item.get(
                    'inventory_item_data', {})]
    if len(player_stats) > 0:
        player_level = player_stats[0].get('level', 1)
        return player_level
    return 0


def parse_account_stats(args, api, response_dict, account):
    # Re-enable pokestops that have been used.
    used_pokestops = dict(account['used_pokestops'])
    for pokestop_id in account['used_pokestops']:
        last_attempt = account['used_pokestops'][pokestop_id]
        if (last_attempt + args.pokestop_refresh_time) < time.time():
            del used_pokestops[pokestop_id]
    account['used_pokestops'] = used_pokestops

    # Check if there are level up rewards to claim.
    if account['first_login']:
        time.sleep(random.uniform(2.0, 3.0))
        if request_level_up_rewards(api, account):
            log.info('Account %s collected its level up rewards.',
                     account['username'])
        else:
            log.info('Account %s already collected level up rewards.',
                     account['username'])

    inventory_items = response_dict['responses'].get(
        'GET_INVENTORY', {}).get(
        'inventory_delta', {}).get(
        'inventory_items', [])
    player_stats = {}
    player_items = {}
    for item in inventory_items:
        item_data = item.get('inventory_item_data', {})
        if 'player_stats' in item_data:
            player_stats = item_data['player_stats']
        elif 'item' in item_data:
            item_id = item_data['item'].get('item_id', 0)
            item_count = item_data['item'].get('count', 0)
            player_items[item_id] = item_count

    log.debug('Account %s items: %s', account['username'], player_items)
    player_level = player_stats.get('level', 0)
    if player_level > 0:
        if player_level > account['level']:
            log.info('Account %s has leveled up! Current level: %d',
                     account['username'], player_level)
            time.sleep(random.uniform(2.0, 3.0))
            if request_level_up_rewards(api, account):
                log.debug('Account %s collected its level up rewards.',
                          account['username'])
            else:
                log.warning('Account %s failed to collect level up rewards.',
                            account['username'])
        else:
            log.debug('Account %s is currently at level %d',
                      account['username'], player_level)
        account['level'] = player_level
        account['items'] = player_items
        return True

    return False


def recycle_items(status, api, account):
    pokeball_count = account['items'].get(1, 0)
    potion_count = account['items'].get(101, 0)

    if pokeball_count > 50:
        drop_count = pokeball_count - 20 - random.randint(5, 10)
        status['message'] = 'Dropping {} Pokeballs.'.format(drop_count)
        log.info(status['message'])
        time.sleep(random.uniform(4.0, 6.0))
        new_count = request_recycle_item(api, 1, drop_count)
        if new_count == -1:
            status['message'] = 'Failed to recycle Pokeballs.'
            log.warning(status['message'])
            return False
        account['items'][1] = new_count

    if potion_count > 30:
        drop_count = potion_count - 10 - random.randint(5, 10)
        status['message'] = 'Dropping {} Potions.'.format(drop_count)
        log.info(status['message'])
        time.sleep(random.uniform(4.0, 6.0))
        new_count = request_recycle_item(api, 101, drop_count)
        if new_count == -1:
            status['message'] = 'Failed to recycle Potions.'
            log.warning(status['message'])
            return False
        account['items'][101] = new_count

    return True


def handle_pokestop(status, api, account, pokestop):
    location = account['last_location']
    pokestop_id = pokestop['pokestop_id']
    if pokestop_id in account['used_pokestops']:
        return False
    if not recycle_items(status, api, account):
        return False

    attempts = 3
    while attempts > 0:
        status['message'] = 'Spinning Pokestop ID: {}'.format(pokestop_id)
        log.info(status['message'])
        time.sleep(random.uniform(2, 3))

        spin_response = request_fort_search(api, pokestop, location)

        # Check for captcha
        captcha_url = spin_response['responses'][
            'CHECK_CHALLENGE']['challenge_url']
        if len(captcha_url) > 1:
            status['message'] = 'Captcha encountered while spinning Pokestop.'
            log.info(status['message'])
            return False

        fort_search = spin_response['responses'].get('FORT_SEARCH', {})
        if 'result' in fort_search:
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

        attempts -= 1
    return False


def request_fort_search(api, pokestop, location):
    try:
        req = api.create_request()
        res = req.fort_search(fort_id=pokestop['pokestop_id'],
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


def get_player_state(api, account):
    try:
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
        tutorial_state = get_player.get(
            'player_data', {}).get('tutorial_state', [])
        account['warning'] = warning_state
        account['tutorials'] = tutorial_state
        time.sleep(random.uniform(1, 3))

        return True

    except Exception as e:
        log.warning('Exception while getting player state: %s', repr(e))

    return False
