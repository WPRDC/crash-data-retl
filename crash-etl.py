"""This script both infers a year to create a table of crash data for (using
the provided filename) and upserts the data to the cumulative table.

This iteration of the crash-etl.py script also converts data from the format received in 2018
(which has the old horrible 0/1 booleans converted into No/Yes values) back to the old horrible
format for consistency.

The best way to use this script now is:

> python crash-etl.py 2004-crashes.csv production
> python crash-etl.py 2005-crashes.csv production
...
> python crash-etl.py 2005-crashes.csv production

and even
> python crash-etl.py 2010-a-few-more-crashes.csv production # [ ] This is inconsistent with how clear_first is defined in the script.

allowing the cumulative resource to be built up from these incremental additions.
"""
import os, sys, json, re, datetime
from marshmallow import fields, pre_load, post_load

sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl
from subprocess import call
import time

import ckanapi

from parameters.local_parameters import SETTINGS_FILE, DATA_PATH
from util.notify import send_to_slack

from pprint import pprint
from icecream import ic

class CrashSchema(pl.BaseSchema): # This schema supports raw lien records
    # (rather than synthesized liens).
    crash_crn = fields.String(dump_to="CRASH_CRN", allow_none=False)
    district = fields.String(dump_to="DISTRICT", allow_none=True)
    crash_county = fields.String(dump_to="CRASH_COUNTY", allow_none=True)
    municipality = fields.String(dump_to="MUNICIPALITY", allow_none=True)
    police_agcy = fields.String(dump_to="POLICE_AGCY", allow_none=True)
    crash_year = fields.Integer(dump_to="CRASH_YEAR", allow_none=True)
    crash_month = fields.String(dump_to="CRASH_MONTH", allow_none=True)
    day_of_week = fields.Integer(dump_to="DAY_OF_WEEK", allow_none=True)
    time_of_day = fields.String(dump_to="TIME_OF_DAY", allow_none=True)
    hour_of_day = fields.String(dump_to="HOUR_OF_DAY", allow_none=True)
    illumination = fields.String(dump_to="ILLUMINATION", allow_none=True)
    weather = fields.String(dump_to="WEATHER", allow_none=True)
    road_condition = fields.String(dump_to="ROAD_CONDITION", allow_none=True)
    collision_type = fields.String(dump_to="COLLISION_TYPE", allow_none=True)
    relation_to_road = fields.String(dump_to="RELATION_TO_ROAD", allow_none=True)
    intersect_type = fields.String(dump_to="INTERSECT_TYPE", allow_none=True)
    tcd_type = fields.String(dump_to="TCD_TYPE", allow_none=True)
    urban_rural = fields.String(dump_to="URBAN_RURAL", allow_none=True)
    location_type = fields.String(dump_to="LOCATION_TYPE", allow_none=True)
    sch_bus_ind = fields.String(dump_to="SCH_BUS_IND", allow_none=True)
    sch_zone_ind = fields.String(dump_to="SCH_ZONE_IND", allow_none=True)
    total_units = fields.Integer(dump_to="TOTAL_UNITS", allow_none=True)
    person_count = fields.Integer(dump_to="PERSON_COUNT", allow_none=True)
    vehicle_count = fields.Integer(dump_to="VEHICLE_COUNT", allow_none=True)
    automobile_count = fields.Integer(dump_to="AUTOMOBILE_COUNT", allow_none=True)
    motorcycle_count = fields.Integer(dump_to="MOTORCYCLE_COUNT", allow_none=True)
    bus_count = fields.Integer(dump_to="BUS_COUNT", allow_none=True)
    small_truck_count = fields.Integer(dump_to="SMALL_TRUCK_COUNT", allow_none=True)
    heavy_truck_count = fields.Integer(dump_to="HEAVY_TRUCK_COUNT", allow_none=True)
    suv_count = fields.Integer(dump_to="SUV_COUNT", allow_none=True)
    van_count = fields.Integer(dump_to="VAN_COUNT", allow_none=True)
    bicycle_count = fields.Integer(dump_to="BICYCLE_COUNT", allow_none=True)
    fatal_count = fields.Integer(dump_to="FATAL_COUNT", allow_none=True)
    injury_count = fields.Integer(dump_to="INJURY_COUNT", allow_none=True)
    maj_inj_count = fields.Integer(dump_to="MAJ_INJ_COUNT", allow_none=True)
    mod_inj_count = fields.Integer(dump_to="MOD_INJ_COUNT", allow_none=True)
    min_inj_count = fields.Integer(dump_to="MIN_INJ_COUNT", allow_none=True)
    unk_inj_deg_count = fields.Integer(dump_to="UNK_INJ_DEG_COUNT", allow_none=True)
    unk_inj_per_count = fields.Integer(dump_to="UNK_INJ_PER_COUNT", allow_none=True)
    unbelted_occ_count = fields.Integer(dump_to="UNBELTED_OCC_COUNT", allow_none=True)
    unb_death_count = fields.Integer(dump_to="UNB_DEATH_COUNT", allow_none=True)
    unb_maj_inj_count = fields.Integer(dump_to="UNB_MAJ_INJ_COUNT", allow_none=True)
    belted_death_count = fields.Integer(dump_to="BELTED_DEATH_COUNT", allow_none=True)
    belted_maj_inj_count = fields.Integer(dump_to="BELTED_MAJ_INJ_COUNT", allow_none=True)
    mcycle_death_count = fields.Integer(dump_to="MCYCLE_DEATH_COUNT", allow_none=True)
    mcycle_maj_inj_count = fields.Integer(dump_to="MCYCLE_MAJ_INJ_COUNT", allow_none=True)
    bicycle_death_count = fields.Integer(dump_to="BICYCLE_DEATH_COUNT", allow_none=True)
    bicycle_maj_inj_count = fields.Integer(dump_to="BICYCLE_MAJ_INJ_COUNT", allow_none=True)
    ped_count = fields.Integer(dump_to="PED_COUNT", allow_none=True)
    ped_death_count = fields.Integer(dump_to="PED_DEATH_COUNT", allow_none=True)
    ped_maj_inj_count = fields.Integer(dump_to="PED_MAJ_INJ_COUNT", allow_none=True)
    comm_veh_count = fields.Integer(dump_to="COMM_VEH_COUNT", allow_none=True)
    max_severity_level = fields.Integer(dump_to="MAX_SEVERITY_LEVEL", allow_none=True)
    driver_count_16yr = fields.Integer(dump_to="DRIVER_COUNT_16YR", allow_none=True)
    driver_count_17yr = fields.Integer(dump_to="DRIVER_COUNT_17YR", allow_none=True)
    driver_count_18yr = fields.Integer(dump_to="DRIVER_COUNT_18YR", allow_none=True)
    driver_count_19yr = fields.Integer(dump_to="DRIVER_COUNT_19YR", allow_none=True)
    driver_count_20yr = fields.Integer(dump_to="DRIVER_COUNT_20YR", allow_none=True)
    driver_count_50_64yr = fields.Integer(dump_to="DRIVER_COUNT_50_64YR", allow_none=True)
    driver_count_65_74yr = fields.Integer(dump_to="DRIVER_COUNT_65_74YR", allow_none=True)
    driver_count_75plus = fields.Integer(dump_to="DRIVER_COUNT_75PLUS", allow_none=True)
    latitude = fields.String(dump_to="LATITUDE", allow_none=True)
    longitude = fields.String(dump_to="LONGITUDE", allow_none=True)
    dec_lat = fields.Float(dump_to="DEC_LAT", allow_none=True)
    dec_long = fields.Float(dump_to="DEC_LONG", allow_none=True)
    est_hrs_closed = fields.Integer(dump_to="EST_HRS_CLOSED", allow_none=True)
    lane_closed = fields.Integer(dump_to="LANE_CLOSED", allow_none=True)
    ln_close_dir = fields.String(dump_to="LN_CLOSE_DIR", allow_none=True)
    ntfy_hiwy_maint = fields.String(dump_to="NTFY_HIWY_MAINT", allow_none=True)
    rdwy_surf_type_cd = fields.String(dump_to="RDWY_SURF_TYPE_CD", allow_none=True)
    spec_juris_cd = fields.String(dump_to="SPEC_JURIS_CD", allow_none=True)
    tcd_func_cd = fields.String(dump_to="TCD_FUNC_CD", allow_none=True)
    tfc_detour_ind = fields.String(dump_to="TFC_DETOUR_IND", allow_none=True)
    work_zone_ind = fields.String(dump_to="WORK_ZONE_IND", allow_none=True)
    work_zone_type = fields.String(dump_to="WORK_ZONE_TYPE", allow_none=True)
    work_zone_loc = fields.String(dump_to="WORK_ZONE_LOC", allow_none=True)
    cons_zone_spd_lim = fields.Integer(dump_to="CONS_ZONE_SPD_LIM", allow_none=True)
    workers_pres = fields.String(dump_to="WORKERS_PRES", allow_none=True)
    wz_close_detour = fields.String(dump_to="WZ_CLOSE_DETOUR", allow_none=True)
    wz_flagger = fields.String(dump_to="WZ_FLAGGER", allow_none=True)
    wz_law_offcr_ind = fields.String(dump_to="WZ_LAW_OFFCR_IND", allow_none=True)
    wz_ln_closure = fields.String(dump_to="WZ_LN_CLOSURE", allow_none=True)
    wz_moving = fields.String(dump_to="WZ_MOVING", allow_none=True)
    wz_other = fields.String(dump_to="WZ_OTHER", allow_none=True)
    wz_shlder_mdn = fields.String(dump_to="WZ_SHLDER_MDN", allow_none=True)
    flag_crn = fields.String(dump_to="FLAG_CRN", allow_none=True)
    interstate = fields.Integer(dump_to="INTERSTATE", allow_none=True)
    state_road = fields.Integer(dump_to="STATE_ROAD", allow_none=True)
    local_road = fields.Integer(dump_to="LOCAL_ROAD", allow_none=True)
    local_road_only = fields.Integer(dump_to="LOCAL_ROAD_ONLY", allow_none=True)
    turnpike = fields.Integer(dump_to="TURNPIKE", allow_none=True)
    wet_road = fields.Integer(dump_to="WET_ROAD", allow_none=True)
    snow_slush_road = fields.Integer(dump_to="SNOW_SLUSH_ROAD", allow_none=True)
    icy_road = fields.Integer(dump_to="ICY_ROAD", allow_none=True)
    sudden_deer = fields.Integer(dump_to="SUDDEN_DEER", allow_none=True)
    shldr_related = fields.Integer(dump_to="SHLDR_RELATED", allow_none=True)
    rear_end = fields.Integer(dump_to="REAR_END", allow_none=True)
    ho_oppdir_sdswp = fields.Integer(dump_to="HO_OPPDIR_SDSWP", allow_none=True)
    hit_fixed_object = fields.Integer(dump_to="HIT_FIXED_OBJECT", allow_none=True)
    sv_run_off_rd = fields.Integer(dump_to="SV_RUN_OFF_RD", allow_none=True)
    work_zone = fields.Integer(dump_to="WORK_ZONE", allow_none=True)
    property_damage_only = fields.Integer(dump_to="PROPERTY_DAMAGE_ONLY", allow_none=True)
    fatal_or_maj_inj = fields.Integer(dump_to="FATAL_OR_MAJ_INJ", allow_none=True)
    injury = fields.Integer(dump_to="INJURY", allow_none=True)
    fatal = fields.Integer(dump_to="FATAL", allow_none=True)
    non_intersection = fields.Integer(dump_to="NON_INTERSECTION", allow_none=True)
    intersection = fields.Integer(dump_to="INTERSECTION", allow_none=True)
    signalized_int = fields.Integer(dump_to="SIGNALIZED_INT", allow_none=True)
    stop_controlled_int = fields.Integer(dump_to="STOP_CONTROLLED_INT", allow_none=True)
    unsignalized_int = fields.Integer(dump_to="UNSIGNALIZED_INT", allow_none=True)
    school_bus = fields.Integer(dump_to="SCHOOL_BUS", allow_none=True)
    school_zone = fields.Integer(dump_to="SCHOOL_ZONE", allow_none=True)
    hit_deer = fields.Integer(dump_to="HIT_DEER", allow_none=True)
    hit_tree_shrub = fields.Integer(dump_to="HIT_TREE_SHRUB", allow_none=True)
    hit_embankment = fields.Integer(dump_to="HIT_EMBANKMENT", allow_none=True)
    hit_pole = fields.Integer(dump_to="HIT_POLE", allow_none=True)
    hit_gdrail = fields.Integer(dump_to="HIT_GDRAIL", allow_none=True)
    hit_gdrail_end = fields.Integer(dump_to="HIT_GDRAIL_END", allow_none=True)
    hit_barrier = fields.Integer(dump_to="HIT_BARRIER", allow_none=True)
    hit_bridge = fields.Integer(dump_to="HIT_BRIDGE", allow_none=True)
    overturned = fields.Integer(dump_to="OVERTURNED", allow_none=True)
    motorcycle = fields.Integer(dump_to="MOTORCYCLE", allow_none=True)
    bicycle = fields.Integer(dump_to="BICYCLE", allow_none=True)
    hvy_truck_related = fields.Integer(dump_to="HVY_TRUCK_RELATED", allow_none=True)
    vehicle_failure = fields.Integer(dump_to="VEHICLE_FAILURE", allow_none=True)
    train_trolley = fields.Integer(dump_to="TRAIN_TROLLEY", allow_none=True)
    phantom_vehicle = fields.Integer(dump_to="PHANTOM_VEHICLE", allow_none=True)
    alcohol_related = fields.Integer(dump_to="ALCOHOL_RELATED", allow_none=True)
    drinking_driver = fields.Integer(dump_to="DRINKING_DRIVER", allow_none=True)
    underage_drnk_drv = fields.Integer(dump_to="UNDERAGE_DRNK_DRV", allow_none=True)
    unlicensed = fields.Integer(dump_to="UNLICENSED", allow_none=True)
    cell_phone = fields.Integer(dump_to="CELL_PHONE", allow_none=True)
    no_clearance = fields.Integer(dump_to="NO_CLEARANCE", allow_none=True)
    running_red_lt = fields.Integer(dump_to="RUNNING_RED_LT", allow_none=True)
    tailgating = fields.Integer(dump_to="TAILGATING", allow_none=True)
    cross_median = fields.Integer(dump_to="CROSS_MEDIAN", allow_none=True)
    curve_dvr_error = fields.Integer(dump_to="CURVE_DVR_ERROR", allow_none=True)
    limit_65mph = fields.Integer(dump_to="LIMIT_65MPH", allow_none=True)
    speeding = fields.Integer(dump_to="SPEEDING", allow_none=True)
    speeding_related = fields.Integer(dump_to="SPEEDING_RELATED", allow_none=True)
    aggressive_driving = fields.Integer(dump_to="AGGRESSIVE_DRIVING", allow_none=True)
    fatigue_asleep = fields.Integer(dump_to="FATIGUE_ASLEEP", allow_none=True)
    driver_16yr = fields.Integer(dump_to="DRIVER_16YR", allow_none=True)
    driver_17yr = fields.Integer(dump_to="DRIVER_17YR", allow_none=True)
    driver_65_74yr = fields.Integer(dump_to="DRIVER_65_74YR", allow_none=True)
    driver_75plus = fields.Integer(dump_to="DRIVER_75PLUS", allow_none=True)
    unbelted = fields.Integer(dump_to="UNBELTED", allow_none=True)
    pedestrian = fields.Integer(dump_to="PEDESTRIAN", allow_none=True)
    distracted = fields.Integer(dump_to="DISTRACTED", allow_none=True)
    curved_road = fields.Integer(dump_to="CURVED_ROAD", allow_none=True)
    driver_18yr = fields.Integer(dump_to="DRIVER_18YR", allow_none=True)
    driver_19yr = fields.Integer(dump_to="DRIVER_19YR", allow_none=True)
    driver_20yr = fields.Integer(dump_to="DRIVER_20YR", allow_none=True)
    driver_50_64yr = fields.Integer(dump_to="DRIVER_50_64YR", allow_none=True)
    vehicle_towed = fields.Integer(dump_to="VEHICLE_TOWED", allow_none=True)
    fire_in_vehicle = fields.Integer(dump_to="FIRE_IN_VEHICLE", allow_none=True)
    hit_parked_vehicle = fields.Integer(dump_to="HIT_PARKED_VEHICLE", allow_none=True)
    mc_drinking_driver = fields.Integer(dump_to="MC_DRINKING_DRIVER", allow_none=True)
    drugged_driver = fields.Integer(dump_to="DRUGGED_DRIVER", allow_none=True)
    injury_or_fatal = fields.Integer(dump_to="INJURY_OR_FATAL", allow_none=True)
    comm_vehicle = fields.Integer(dump_to="COMM_VEHICLE", allow_none=True)
    impaired_driver = fields.Integer(dump_to="IMPAIRED_DRIVER", allow_none=True)
    deer_related = fields.Integer(dump_to="DEER_RELATED", allow_none=True)
    drug_related = fields.Integer(dump_to="DRUG_RELATED", allow_none=True)
    hazardous_truck = fields.Integer(dump_to="HAZARDOUS_TRUCK", allow_none=True)
    illegal_drug_related = fields.Integer(dump_to="ILLEGAL_DRUG_RELATED", allow_none=True)
    illumination_dark = fields.Integer(dump_to="ILLUMINATION_DARK", allow_none=True)
    minor_injury = fields.Integer(dump_to="MINOR_INJURY", allow_none=True)
    moderate_injury = fields.Integer(dump_to="MODERATE_INJURY", allow_none=True)
    major_injury = fields.Integer(dump_to="MAJOR_INJURY", allow_none=True)
    nhtsa_agg_driving = fields.Integer(dump_to="NHTSA_AGG_DRIVING", allow_none=True)
    psp_reported = fields.Integer(dump_to="PSP_REPORTED", allow_none=True)
    running_stop_sign = fields.Integer(dump_to="RUNNING_STOP_SIGN", allow_none=True)
    train = fields.Integer(dump_to="TRAIN", allow_none=True)
    trolley = fields.Integer(dump_to="TROLLEY", allow_none=True)
    roadway_crn = fields.String(dump_to="ROADWAY_CRN", allow_none=True)
    rdwy_seq_num = fields.Integer(dump_to="RDWY_SEQ_NUM", allow_none=True)
    adj_rdwy_seq = fields.Integer(dump_to="ADJ_RDWY_SEQ", allow_none=True)
    access_ctrl = fields.String(dump_to="ACCESS_CTRL", allow_none=True)
    roadway_county = fields.String(dump_to="ROADWAY_COUNTY", allow_none=True)
    lane_count = fields.Integer(dump_to="LANE_COUNT", allow_none=True)
    rdwy_orient = fields.String(dump_to="RDWY_ORIENT", allow_none=True)
    road_owner = fields.String(dump_to="ROAD_OWNER", allow_none=True)
    route = fields.String(dump_to="ROUTE", allow_none=True)
    speed_limit = fields.Integer(dump_to="SPEED_LIMIT", allow_none=True)
    segment = fields.String(dump_to="SEGMENT", allow_none=True)
    offset= fields.Integer(dump_to="OFFSET", allow_none=True)
    street_name = fields.String(dump_to="STREET_NAME", allow_none=True)

    # Never let any of the key fields have None values. It's just asking for
    # multiplicity problems on upsert.

    # [Note that since this script is taking data from CSV files, there should be no
    # columns with None values. It should all be instances like [value], [value],, [value],...
    # where the missing value starts as a zero-length string, which this script
    # is then responsible for converting into something more appropriate.

    # Ah, but now (2019) there can be None values since 3 columns are no longer
    # being supported from this schema. Since these values were never None in
    # the 2016 data (and presumably other sets), it seems OK to allow these
    # values to be None (rather than shrinking the schema for every year that
    # the State DOT stops publishing certain columns, it will only expand
    # and certain values may just not be defined for certain years, as this
    # will make analyzing the data easier for the user).

    class Meta:
        ordered = True

    # From the Marshmallow documentation:
    #   Warning: The invocation order of decorated methods of the same
    #   type is not guaranteed. If you need to guarantee order of different
    #   processing steps, you should put them in the same processing method.
    @pre_load
    def fix_types(self, data):
        # Fixing of types is necessary since the 2016 data got typed
        # differently.
        if data['est_hrs_closed'] is not None:
            data['est_hrs_closed'] = int(float(data['est_hrs_closed']))
        if data['cons_zone_spd_lim'] is not None:
            data['cons_zone_spd_lim'] = int(float(data['cons_zone_spd_lim']))

        #    data['party_type'] = '' # If you make these values
        #    # None instead of empty strings, CKAN somehow
        #    # interprets each None as a different key value,
        #    # so multiple rows will be inserted under the same
        #    # DTD/tax year/lien description even though the
        #    # property owner has been redacted.
        #    data['party_name'] = ''
        #    #data['party_first'] = '' # These need to be referred
        #    # to by their schema names, not the name that they
        #    # are ultimately dumped to.
        #    #data['party_middle'] = ''
        #    data['plaintiff'] = '' # A key field can not have value
        #    # None or upserts will work as blind inserts.
        #else:
        #    data['plaintiff'] = str(data['party_name'])
        #del data['party_type']
        #del data['party_name']
    # The stuff below was originally written as a separate function
    # called avoid_null_keys, but based on the above warning, it seems
    # better to merge it with omit_owners.

# Resource Metadata
#package_id = '626e59d2-3c0e-4575-a702-46a71e8b0f25'     # Production
#package_id = '85910fd1-fc08-4a2d-9357-e0692f007152'     # Stage
###############
# FOR SOME PART OF THE BELOW PIPELINE, I THINK...
#The package ID is obtained not from this file but from
#the referenced settings.json file when the corresponding
#flag below is True.

class ExtendedCrashSchema(CrashSchema):
    tot_inj_count = fields.Integer(dump_to="TOT_INJ_COUNT", allow_none=True)
    school_bus_unit = fields.String(dump_to="SCHOOL_BUS_UNIT", allow_none=True)

def get_package_parameter(site,package_id,parameter,API_key=None):
    # Some package parameters you can fetch from the WPRDC with
    # this function are:
    # 'geographic_unit', 'owner_org', 'maintainer', 'data_steward_email',
    # 'relationships_as_object', 'access_level_comment',
    # 'frequency_publishing', 'maintainer_email', 'num_tags', 'id',
    # 'metadata_created', 'group', 'metadata_modified', 'author',
    # 'author_email', 'state', 'version', 'department', 'license_id',
    # 'type', 'resources', 'num_resources', 'data_steward_name', 'tags',
    # 'title', 'frequency_data_change', 'private', 'groups',
    # 'creator_user_id', 'relationships_as_subject', 'data_notes',
    # 'name', 'isopen', 'url', 'notes', 'license_title',
    # 'temporal_coverage', 'related_documents', 'license_url',
    # 'organization', 'revision_id'
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = ckan.action.package_show(id=package_id)
        desired_string = metadata[parameter]
        #print("The parameter {} for this package is {}".format(parameter,metadata[parameter]))
    except:
        raise RuntimeError("Unable to obtain package parameter '{}' for package with ID {}".format(parameter,package_id))
    #
    return desired_string

def find_resource_id(site,package_id,resource_name,API_key=None):
    # Get the resource ID given the package ID and resource name.
    resources = get_package_parameter(site,package_id,'resources',API_key)
    for r in resources:
        if r['name'] == resource_name:
            return r['id']
    return None

def main(*args,**kwparams):

    target = kwparams.get('filename',None)
    if target is None:
        raise ValueError("Unable to process data without filename.")

    pathlist = target.split('/')
    fname = pathlist[-1]

    resource_id = kwparams.get('resource_id',None)
    if resource_id is None:
        try:
            year = int(fname[:4])
        except ValueError:
            raise ValueError("The first four characters of the file name have to be integers to allow the year to be extracted from the file name.")

    # Pick schema based on the presence or absence of certain fields (that showed up in the 2017 data).
    with open(target,'r') as f:
        headers = f.readline().strip()

    fieldnames = headers.split(',')

    if 'TOT_INJ_COUNT' in fieldnames or 'SCHOOL_BUS_UNIT' in fieldnames:
        schema = ExtendedCrashSchema
        print("Using the extended schema to accommodate extra fields for 2017 data.")
        print("Note that though 2018 data dropped three fields (ACCESS_CTRL, ADJ_RDWY_SEQ, and LOCAL_ROAD),")
        print("we'll just keep using the same extended schema.")
    else:
        schema = CrashSchema
    fields0 = schema().serialize_to_ckan_fields()
    # Eliminate fields that we don't want to upload.
    #fields0.pop(fields0.index({'type': 'text', 'id': 'party_type'}))
    #fields0.pop(fields0.index({'type': 'text', 'id': 'party_name'}))
    #fields0.append({'id': 'assignee', 'type': 'text'})
    fields_to_publish = fields0
    #print("fields_to_publish = {}".format(fields_to_publish))

    #target = '/Users/daw165/data/TaxLiens/July31_2013/raw-liens.csv' # This path is hard-coded.

    # Call function that converts fixed-width file into a CSV file. The function
    # returns the target file path.

    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    #os.chdir(dname)

    #server = "production"
    #server = "test"
    server = kwparams.get('server',None)
    # Code below stolen from prime_ckan/*/open_a_channel() but really from utility_belt/gadgets
    #with open(os.path.dirname(os.path.abspath(__file__))+'/ckan_settings.json') as f: # The path of this file needs to be specified.

    with open(SETTINGS_FILE) as f:
        settings = json.load(f)
    site = settings['loader'][server]['ckan_root_url']
    package_id = settings['loader'][server]['package_id']
    API_key = settings['loader'][server]['ckan_api_key']

    specify_resource_by_name = (resource_id is None)

    kwargs = {}
    if specify_resource_by_name:
        kwargs['resource_name'] = kwparams.get('resource_name','{} Crash Data'.format(year))
        # Set clear_first based on whether the resource is already there.
        resource_id = find_resource_id(site,package_id,kwargs['resource_name'],API_key)
    else:
        kwargs['resource_id'] = resource_id

    clear_first = (resource_id is not None)
    print("resource_id = {}, clear_first = {}".format(resource_id,clear_first))
    #resource_id = '8cd32648-757c-4637-9076-85e144997ca8' # Raw liens

    print("Preparing to pipe data from {} to resource {} package ID {} on {}".format(target,list(kwargs.values())[0],package_id,site))
    time.sleep(1.0)


    the_pipeline = pl.Pipeline('crash_data_pipeline',
                                      'The Long-Awaited Pipeline for the Crash Data',
                                      log_status=False,
                                      settings_file=SETTINGS_FILE,
                                      settings_from_file=True,
                                      start_from_chunk=0,
                                      chunk_size=2000
                                      )
    pipeline_ok = the_pipeline.connect(pl.FileConnector, target, encoding='utf-8') \
        .extract(pl.CSVExtractor, firstline_headers=True) \
        .schema(schema) \
        .load(pl.CKANDatastoreLoader, server,
              fields=fields_to_publish,
              #package_id=package_id,
              #resource_id=resource_id,
              #resource_name=resource_name,
              key_fields=['CRASH_CRN'],
              clear_first=clear_first,
              method='upsert',
              **kwargs).run()
    log = open('uploaded.log', 'w+')
    if specify_resource_by_name:
        print("Piped data to {}".format(kwargs['resource_name']))
        log.write("Finished upserting data to {}\n".format(kwargs['resource_name']))
    else:
        print("Piped data to {}".format(kwargs['resource_id']))
        log.write("Finished upserting data to {}\n".format(kwargs['resource_id']))
    log.close()

    ###### Cumulative Pipeline ###########
    schema = ExtendedCrashSchema
    kwargs = {}
    specify_resource_by_name = False
    cumulative_resource_id = "2c13021f-74a9-4289-a1e5-fe0472c89881"
    kwargs['resource_id'] = cumulative_resource_id

    cumulative_pipeline = pl.Pipeline('cumulative_crash_data_pipeline',
                                      'The Cumulative Pipeline for the Crash Data Which You Thought Would Never Come',
                                      log_status=False,
                                      settings_file=SETTINGS_FILE,
                                      settings_from_file=True,
                                      start_from_chunk=0,
                                      chunk_size=2000
                                      )
    cumulative_pipeline_ok = cumulative_pipeline.connect(pl.FileConnector, target, encoding='utf-8') \
        .extract(pl.CSVExtractor, firstline_headers=True) \
        .schema(schema) \
        .load(pl.CKANDatastoreLoader, server,
              fields=fields_to_publish,
              #package_id=package_id,
              #resource_id=resource_id,
              #resource_name=resource_name,
              key_fields=['CRASH_CRN'],
              clear_first=False, # This is different for the cumulative pipeline.
              method='upsert',
              **kwargs).run()

    if specify_resource_by_name:
        print("Piped data to {}".format(kwargs['resource_name']))
    else:
        print("Piped data to {}".format(kwargs['resource_id']))


if __name__ == "__main__":
   # stuff only to run when not called via 'import' here
    if len(sys.argv) == 2:
        main(filename = sys.argv[1], server='test') # If no server is specified, the test server will be used by default.
    elif len(sys.argv) == 3:
        main(filename = sys.argv[1], server=sys.argv[2])
    elif len(sys.argv) == 4:
        main(filename = sys.argv[1], server=sys.argv[2], resource_id=sys.argv[3])
    else:
        print("There are either too many or too few command-line arguments.")
