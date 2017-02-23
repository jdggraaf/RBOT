#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import time

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


def spin_pokestop(args, status, api, account, account_failures,
                  account_captchas, whq, response_dict, location, pokestop):

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
