#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import time
import random

from pgoapi.exceptions import AuthException

log = logging.getLogger(__name__)

SPIN_REQUEST_RESULT_SUCCESS = 1
SPIN_REQUEST_RESULT_OUT_OF_RANGE = 2
SPIN_REQUEST_RESULT_IN_COOLDOWN_PERIOD = 3
SPIN_REQUEST_RESULT_INVENTORY_FULL = 4
SPIN_REQUEST_RESULT_MAXIMUM_REACHED = 5

MAX_DISTANCE_FORT_IS_REACHABLE = 38     # In meters.
MAX_DISTANCE_POKEMON_IS_REACHABLE = 48  # In meters.

ITEM_POKEBALL = 1
ITEM_GREATBALL = 2
ITEM_ULTRABALL = 3
ITEM_RAZZBERRY = 701
ITEM_POTION = 101
ITEM_SUPER_POTION = 102
ITEM_HYPER_POTION = 103
ITEM_MAX_POTION = 104
ITEM_REVIVE = 201
ITEM_MAX_REVIVE = 202
POTIONS = [ITEM_POTION, ITEM_SUPER_POTION, ITEM_HYPER_POTION,
           ITEM_MAX_POTION, ITEM_REVIVE, ITEM_MAX_REVIVE]


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
def complete_tutorial(api, account, tutorial_state):
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


def spin_pokestop(status, api, location, pokestop):

    status['message'] = 'Trying to drop a Pokeball...'
    log.info(status['message'])
    time.sleep(10)
    req = api.create_request()
    response_dict = req.recycle_inventory_item(item_id=1, count=1)
    response_dict = req.call()
    # TODO: remove
    log.debug(response_dict)
    if ('responses' in response_dict) and (
            'RECYCLE_INVENTORY_ITEM' in response_dict['responses']):
        drop_details = response_dict['responses']['RECYCLE_INVENTORY_ITEM']
        drop_result = drop_details.get('result', -1)
        if (drop_result == 1):
            status['message'] = 'Dropped a Pokeball.'
            log.info(status['message'])
        else:
            status['message'] = 'Unable to drop Pokeball.'
            log.warning(status['message'])
    time.sleep(5)
    req = api.create_request()
    spin_response = req.fort_search(fort_id=pokestop['id'],
                                    fort_latitude=pokestop['latitude'],
                                    fort_longitude=pokestop['longitude'],
                                    player_latitude=location['latitude'],
                                    player_longitude=location['longitude'])

    spin_response = req.check_challenge()
    spin_response = req.get_hatched_eggs()
    spin_response = req.get_inventory()
    spin_response = req.check_awarded_badges()
    spin_response = req.download_settings()
    spin_response = req.get_buddy_walked()
    spin_response = req.call()
    # TODO: remove
    log.debug(spin_response)

    # Check for captcha
    captcha_url = spin_response['responses'][
        'CHECK_CHALLENGE']['challenge_url']
    if len(captcha_url) > 1:
        status['message'] = 'Captcha encountered when spinning pokestop.'
        log.info(status['message'])
        return False
    if ('responses' in spin_response) and (
            'FORT_SEARCH' in response_dict['responses']):
            spin_details = response_dict['responses']['FORT_SEARCH']
            spin_result = spin_details.get('result', -1)
            if (spin_result == SPIN_REQUEST_RESULT_SUCCESS) or (
                    spin_result == SPIN_REQUEST_RESULT_INVENTORY_FULL):
                experience_awarded = spin_details.get('experience_awarded', 0)
                if experience_awarded:
                    log.info("Spun pokestop got response data!")
                    return True
                else:
                    log.info('Found nothing in pokestop')
            elif spin_result == SPIN_REQUEST_RESULT_OUT_OF_RANGE:
                log.info("Pokestop out of range.")
            elif spin_result == SPIN_REQUEST_RESULT_IN_COOLDOWN_PERIOD:
                log.info("Pokestop is on cooldown.")
            elif spin_result == SPIN_REQUEST_RESULT_MAXIMUM_REACHED:
                log.info("Pokestop maximum daily quota reached.")
            else:
                log.warning("Unable to spin Pokestop, unknown return: %s",
                            spin_result)
    return False


def check_level(response_dict):
    inventory_items = response_dict['responses'].get('GET_INVENTORY', {}).get(
        'inventory_delta', {}).get(
        'inventory_items', [])
    log.debug(inventory_items)
    player_stats = [item['inventory_item_data']['player_stats']
                    for item in inventory_items
                    if 'player_stats' in item.get('inventory_item_data', {})]
    if len(player_stats) > 0:
        return player_stats[0].get('level', 0)

    return -1
