import os
import sys
import csv
import math
import time
import copy
import pickle
import numpy
import pandapower as pp
from pandas import options as pdoptions
import data as data_read

cwd = os.path.dirname(__file__)

# -- DEVELOPMENT DEFAULT ------------------------------------------------------
if not sys.argv[1:]:
    con_fname = cwd + r'/sandbox/Network_01-10O/scenario_1/case.con'
    inl_fname = cwd + r'/sandbox/Network_01-10O/case.inl'
    raw_fname = cwd + r'/sandbox/Network_01-10O/scenario_1/case.raw'
    rop_fname = cwd + r'/sandbox/Network_01-10O/case.rop'
    outfname1 = cwd + r'/sandbox/Network_01-10O/scenario_1/solution1.txt'
    outfname2 = cwd + r'/sandbox/Network_01-10O/scenario_1/solution2.txt'

# -- USING COMMAND LINE -------------------------------------------------------
if sys.argv[1:]:
    print()
    con_fname = sys.argv[1]
    inl_fname = sys.argv[2]
    raw_fname = sys.argv[3]
    rop_fname = sys.argv[4]
    outfname1 = 'solution1.txt'
    outfname2 = 'solution2.txt'

try:
    os.remove(outfname1)
except FileNotFoundError:
    pass

GVREG = 0                   # GENERATORS VOLTAGE SCHEDULES ........  0=DEFAULT_GENV_RAW, 1=CUSTOM(ALL)
SWSHVREG = 1                # SWITCHED SHUNTS VOLTAGE SCHEDULES ...  0=DEFAULT_BUSV_RAW, 1=CUSTOM(ALL)
Gvreg_Custom = 1.00
SwShVreg_Custom = 1.025

# =============================================================================
# -- FUNCTIONS ----------------------------------------------------------------
# =============================================================================
def listoflists(tt):
    """ convert tuple_of_tuples to list_of_lists """
    return list((listoflists(x) if isinstance(x, tuple) else x for x in tt))


def tupleoftuples(ll):
    """ convert list_of_lists to tuple_of_tuples """
    return tuple((tupleoftuples(x) if isinstance(x, list) else x for x in ll))


def read_data(raw_name, rop_name, inl_name, con_name):
    p = data_read.Data()
    print('READING RAW FILE ...................................................', os.path.split(raw_name)[1])
    if raw_name is not None:
        p.raw.read(os.path.normpath(raw_name))
    print('READING ROP FILE ...................................................', os.path.split(rop_name)[1])
    if rop_name is not None:
        p.rop.read(os.path.normpath(rop_name))
    print('READING INL FILE ...................................................', os.path.split(inl_name)[1])
    if inl_name is not None:
        p.inl.read(os.path.normpath(inl_name))
    print('READING CON FILE....................................................', os.path.split(con_name)[1])
    if con_name is not None:
        p.con.read(os.path.normpath(con_name))
    return p


def get_swingbus_data(buses):
    # buses = (bus.i, bus.ide, bus.baskv, bus.area, bus.vm, bus.va, bus.nvhi, bus.nvlo, bus.evhi, bus.evlo)
    swbus = None
    swangle = 0.0
    for bus in buses.values():
        if bus.ide == 3:
            swbus = bus.i
            swkv = bus.baskv
            swangle = 0.0
            swvhigh = bus.evhi
            swvlow = bus.evlo
            break
    return [swbus, swkv, swangle, swvlow, swvhigh]


def get_swgens_data(swbus, generators):
    # generators = (gen.i, gen.id, gen.pg, gen.qg, gen.qt, gen.qb, gen.vs, gen.pt, gen.pb, gen.stat)
    swgens_data = []
    del_gens = []
    for gen in generators.values():
        if gen.i == swbus:
            gkey = str(gen.i) + '-' + gen.id
            swgens_data.append([gkey, str(gen.id), float(gen.pg), float(gen.qg), float(gen.qt), float(gen.qb), float(gen.vs), float(gen.pt), float(gen.pb)])
            del_gens.append((gen.i, gen.id))
    for gen in del_gens:
        del generators[gen]
    return swgens_data


def get_alt_swingbus(generators, buses, swbus, sw_kv, sw_reg):
    # generators = (gen.i, gen.id, gen.pg, gen.qg, gen.qt, gen.qb, gen.vs, gen.pt, gen.pb, gen.stat)
    # buses = (bus.i, bus.ide, bus.baskv, bus.area, bus.vm, bus.va, bus.nvhi, bus.nvlo, bus.evhi, bus.evlo)
    swbus1 = None
    sw1gkey = None
    maxp_list = [[g.pt, g.i] for g in generators.values()]
    maxp_list.sort(reverse=True)
    for g in maxp_list:
        gbus = g[1]
        for bus in buses.values():
            if bus.i == gbus and bus.baskv == sw_kv:
                swbus1 = bus.i
                for g in generators.values():
                    if g.i == gbus:
                        g.vs = sw_reg
                        sw1gkey = str(g.i) + '-' + g.id
                        return swbus1, sw1gkey
    return swbus1, sw1gkey


def write_csvdata(fname, lol, label):
    with open(fname, 'a', newline='') as fobject:
        writer = csv.writer(fobject, delimiter=',', quotechar='"')
        for j in label:
            writer.writerow(j)
        writer.writerows(lol)
    fobject.close()
    return


def write_base_bus_results(fname, b_results, sw_dict, g_results, exgridbus):
    # -- DELETE UNUSED DATAFRAME COLUMNS --------------------------------------
    try:
        del b_results['p_kw']        # not used for reporting
    except KeyError:
        pass
    try:
        del b_results['q_kvar']      # not used for reporting
    except KeyError:
        pass
    try:
        del b_results['lam_p']       # not used for reporting
    except KeyError:
        pass
    try:
        del b_results['lam_q']       # not used for reporting
    except KeyError:
        pass
    # -- REMOVE EXTERNAL GRID BUS RESULTS -------------------------------------
    b_results.drop([exgridbus], inplace=True)
    # -- ADD BUSNUMBER COLUMN -------------------------------------------------
    b_results.insert(0, 'bus', b_results.index)
    # -- ADD SHUNT MVARS COLUMN (FILLED WITH 0.0) -----------------------------
    b_results['sh_mvars'] = 0.0
    # -- RENAME COLUMN HEADINGS -----------------------------------------------
    b_results.rename(columns={'vm_pu': 'voltage_pu', 'va_degree': 'angle'}, inplace=True)
    # -- PREVENT NEGATIVE ZEROS -----------------------------------------------
    b_results['voltage_pu'] += 0.0
    b_results['angle'] += 0.0
    # -- CONVERT PANDAS DATAFRAME TO LIST FOR REPORTING -----------------------
    buslist = [b_results.columns.values.tolist()] + b_results.values.tolist()
    # -- GET ANY SHUNT MVARS FOR REPORTING ------------------------------------
    # -- (SWITCHED SHUNTS ARE MODELED AS GENERATORS) --------------------------
    for j in range(1, len(buslist)):
        buslist[j][0] = int(buslist[j][0])
        bus = buslist[j][0]
        mvars = 0.0
        if bus in sw_dict:
            mvars = -1e-3 * g_results.loc[sw_dict[bus], 'q_kvar'] / (b_results.loc[bus, 'voltage_pu'] ** 2)
            buslist[j][3] = mvars + 0.0
    # -- WRITE THE BUS RESULTS TO FILE ----------------------------------------
    write_csvdata(fname, buslist, [['--bus section']])
    return


def write_base_gen_results(fname, g_results, genids, gbuses, swsh_idxs):
    g_results.drop(swsh_idxs, inplace=True)
    del g_results['vm_pu']
    del g_results['va_degree']
    # -- CONVERT BACK TO MW AND MVARS -----------------------------------------
    g_results['p_kw'] *= -1e-3
    g_results['q_kvar'] *= -1e-3
    g_results['p_kw'] += 0.0
    g_results['q_kvar'] += 0.0
    # -- RENAME COLUMN HEADINGS -----------------------------------------------
    g_results.rename(columns={'p_kw': 'mw', 'q_kvar': 'mvar'}, inplace=True)
    # -- ADD GENERATOR BUSNUMBERS AND IDS -------------------------------------
    g_results.insert(0, 'id', genids)
    g_results.insert(0, 'bus', gbuses)
    # -- CONVERT PANDAS DATAFRAME TO LIST FOR REPORTING -----------------------
    glist = [g_results.columns.values.tolist()] + g_results.values.tolist()
    # -- WRITE THE GENERATION RESULTS TO FILE ---------------------------------
    write_csvdata(fname, glist, [['--generator section']])
    return


# def write_bus_results(fname, b_results, sw_dict, g_results, exgridbus, clabel):
#     # -- DELETE UNUSED DATAFRAME COLUMNS --------------------------------------
#     try:
#         del b_results['p_kw']        # not used for reporting
#     except KeyError:
#         pass
#     try:
#         del b_results['q_kvar']      # not used for reporting
#     except KeyError:
#         pass
#     try:
#         del b_results['lam_p']       # not used for reporting
#     except KeyError:
#         pass
#     try:
#         del b_results['lam_q']       # not used for reporting
#     except KeyError:
#         pass
#     # -- REMOVE EXTERNAL GRID BUS RESULTS -------------------------------------
#     b_results.drop([exgridbus], inplace=True)
#     # -- ADD BUSNUMBER COLUMN -------------------------------------------------
#     b_results.insert(0, 'bus', b_results.index)
#     # -- ADD SHUNT MVARS COLUMN (FILLED WITH 0.0) -----------------------------
#     b_results['shunt_mvars@1pu'] = 0.0
#     # -- RENAME COLUMN HEADINGS -----------------------------------------------
#     b_results.rename(columns={'vm_pu': 'pu_voltage', 'va_degree': 'angle'}, inplace=True)
#     # -- PREVENT NEGATIVE ZEROS -----------------------------------------------
#     b_results['voltage'] += 0.0
#     b_results['angle'] += 0.0
#     # -- CONVERT PANDAS DATAFRAME TO LIST FOR REPORTING -----------------------
#     buslist = [b_results.columns.values.tolist()] + b_results.values.tolist()
#     # -- GET ANY SHUNT MVARS FOR REPORTING ------------------------------------
#     # -- (SWITCHED SHUNTS ARE MODELED AS GENERATORS) --------------------------
#     for j in range(1, len(buslist)):
#         buslist[j][0] = int(buslist[j][0])
#         bus = buslist[j][0]
#         mvars = 0.0
#         if bus in sw_dict:
#             # mvars = -1e-3 * g_results.loc[sw_dict[bus], 'q_kvar']
#             mvars = -1e-3 * g_results.loc[sw_dict[bus], 'q_kvar'] / (b_results.loc[bus, 'pu_voltage'] ** 2)
#             buslist[j][3] = mvars + 0.0
#     # -- WRITE THE BUS RESULTS TO FILE ----------------------------------------
#     write_csvdata(fname, [], [['--contingency'], ['label'], [clabel]])
#     write_csvdata(fname, buslist, [['--bus section']])
#     return
#
#
# def write_gen_results(fname, g_results, genids, gbuses, delta, swsh_idxs):
#     g_results.drop(swsh_idxs, inplace=True)
#     del g_results['vm_pu']
#     del g_results['va_degree']
#     # -- CONVERT BACK TO MW AND MVARS -----------------------------------------
#     g_results['p_kw'] *= -1e-3
#     g_results['q_kvar'] *= -1e-3
#     g_results['p_kw'] += 0.0
#     g_results['q_kvar'] += 0.0
#     delta *= -1e-3
#     # pgen_out *= -1e-3
#     # -- RENAME COLUMN HEADINGS -----------------------------------------------
#     g_results.rename(columns={'p_kw': 'mw', 'q_kvar': 'mvar'}, inplace=True)
#     # -- ADD GENERATOR BUSNUMBERS AND IDS -------------------------------------
#     g_results.insert(0, 'id', genids)
#     g_results.insert(0, 'bus', gbuses)    # -- CALCULATE TOTAL POWER OF PARTICIPATING GENERATORS --------------------
#     # c_gens = sum([x for x in g_results['mw'].values])
#     # -- CONVERT PANDAS DATAFRAME TO LIST FOR REPORTING -----------------------
#     glist = [g_results.columns.values.tolist()] + g_results.values.tolist()
#     # -- WRITE THE GENERATION RESULTS TO FILE ---------------------------------
#     write_csvdata(fname, glist, [['--generator section']])
#     # deltapgens = p_delta + pgen_out
#     write_csvdata(fname, [], [['--delta section'], ['delta_p'], [delta]])
#     return


def print_dataframes_results(_net):
    pdoptions.display.max_columns = 100
    pdoptions.display.max_rows = 1000
    pdoptions.display.max_colwidth = 100
    pdoptions.display.width = None
    pdoptions.display.precision = 4
    # print()
    # print('BUS DATAFRAME')
    # print(_net.bus)
    # print()
    # print('BUS RESULTS')
    # print(_net.res_bus)
    # print()
    # print('LOAD DATAFRAME')
    # print(_net.load)
    # print()
    # print('FXSHUNT DATAFRAME')
    # print(_net.shunt)
    # print()
    # print('FXSHUNT RESULTS')
    # print(_net.res_shunt)
    # print()
    # print('LINE DATAFRAME')
    # print(_net.line)
    # print()
    # print('LINE RESULTS')
    # print(_net.res_line)
    # print('MAX LINE LOADING % =', max(_net.res_line['loading_percent'].values))
    # print()
    # print('TRANSFORMER DATAFRAME')
    # print(_net.trafo)
    # print()
    # print('TRANSFORMER RESULTS')
    # print(_net.res_trafo)
    # print('MAX XFMR LOADING % =', max(_net.res_trafo['loading_percent'].values))
    print()
    print('GENERATOR DATAFRAME')
    print(_net.gen)
    print()
    print('GENERATOR RESULTS')
    print(_net.res_gen)
    print()
    print('EXT GRID DATAFRAME')
    print(_net.ext_grid)
    print()
    print('EXT GRID RESULTS')
    print(_net.res_ext_grid)
    print()
    return


def get_branch_losses(line_res,  trafo_res):
    losses = 0.0
    pfrom = line_res['p_from_kw'].values
    pto = line_res['p_to_kw'].values
    line_losses = numpy.add(pfrom, pto)
    line_losses = [abs(x) for x in line_losses]
    line_losses = sum(line_losses)
    losses += line_losses
    pfrom = trafo_res['p_hv_kw'].values
    pto = trafo_res['p_lv_kw'].values
    xfmr_losses = numpy.add(pfrom, pto)
    xfmr_losses = [abs(x) for x in xfmr_losses]
    xfmr_losses = sum(xfmr_losses)
    losses += xfmr_losses
    return losses


# =================================================================================================
# -- MYPYTHON_1 -----------------------------------------------------------------------------------
# =================================================================================================
if __name__ == "__main__":
    print()
    cwd = os.getcwd()
    start_time = time.time()

    # =============================================================================================
    # -- GET RAW,ROP,INL,CON DATA FROM FILES ------------------------------------------------------
    # =============================================================================================
    print('------------------------- READING RAW DATA -------------------------')
    areas = []
    gdisp_dict = {}
    pdisp_dict = {}
    pwlcost_dict0 = {}
    pwlcost_dict = {}
    outage_dict = {'branch': {}, 'gen': {}}
    pfactor_dict = {}
    pwl_map_dict = {}
    max_pwl_shape = 0
    raw_data = read_data(raw_fname, rop_fname, inl_fname, con_fname)

    # -- GET BASE MVA -----------------------------------------------------------------------------
    mva_base = raw_data.raw.case_identification.sbase

    # -- GET NETWORK AREAS ------------------------------------------------------------------------
    for area in raw_data.raw.areas.values():
        areas.append(area.i)

    # -- GET ACTIVE POWER DISPATCH TABLES FROM GEN DISPATCH DATA ----------------------------------
    for gdisp in raw_data.rop.generator_dispatch_records.values():
        gkey = str(gdisp.bus) + '-' + str(gdisp.genid)
        gdisp_dict.update({gkey: gdisp.dsptbl})

    # -- GET UNIQUE PWL DATA FROM PWL COST TABLES (AND MAP DUPLICATES) ----------------------------
    for pwldata in raw_data.rop.piecewise_linear_cost_functions.values():
        pwlcost_dict0.update({pwldata.ltbl: []})
        # -- HACK - JUST USE END POINTS -----------------------------------------------------------
        pwlcost_dict0[pwldata.ltbl].append([-1e3 * pwldata.points[0].x, pwldata.points[0].y])
        pwlcost_dict0[pwldata.ltbl].append([-1e3 * pwldata.points[-1].x, pwldata.points[-1].y])
        # for pair in pwldata.points:
        #     pwlcost_dict0[pwldata.ltbl].append([-1e3 * pair.x, pair.y])
        pwlcost_dict0[pwldata.ltbl].sort()
        pwlcost_dict0[pwldata.ltbl][0][0] -= 0.1
        pwlcost_dict0[pwldata.ltbl][-1][0] += 0.1
    for key, pwl_list in pwlcost_dict0.items():
        if len(pwl_list) > max_pwl_shape:
            max_pwl_shape = len(pwl_list)
        if pwl_list not in pwlcost_dict.values():
            pwlcost_dict.update({key: pwl_list})
            pwl_map_dict.update({key: key})
        else:
            for k, lst in pwlcost_dict.items():
                if lst == pwl_list:
                    pwl_map_dict.update({key: k})

    # -- MAKE SURE ALL PWL DATA HAS SAME SHAPE ----------------------------------------------------
    for key, pwl_list in pwlcost_dict.items():
        difference = max_pwl_shape - len(pwlcost_dict[key])
        for j in range(difference):
            x = (pwlcost_dict[key][0][0] + pwlcost_dict[key][1][0]) / 2
            y = (pwlcost_dict[key][0][1] + pwlcost_dict[key][1][1]) / 2
            pwlcost_dict[key].append([x, y])
            pwlcost_dict[key].sort()

    # -- GET PWL TABLES FROM ACTIVE POWER DISPATCH TABLES -----------------------------------------
    for pdisp in raw_data.rop.active_power_dispatch_records.values():
        pdisp_dict.update({pdisp.tbl: pwl_map_dict[pdisp.ctbl]})

    # -- TODO CHECK OR FORCE PWL DATA IS CONVEX ---------------------------------------------------
    # for key, pwl_list in pwlcost_dict.items():
    #     print()
    #     print(key)
    #     print(pwl_list)
    #     oldslope = -99.0
    #     for j in range(len(pwl_list) - 1):
    #         slope = (pwl_list[j+1][1] - pwl_list[j][1]) / (pwl_list[j+1][0] - pwl_list[j][0])
    #         print(slope, slope > oldslope)
    #         oldslope = slope

    # -- GET CONTINGENCY DATA ---------------------------------------------------------------------
    for con in raw_data.con.contingencies.values():
        clabel = con.label
        for event in con.branch_out_events:
            ibus = event.i
            jbus = event.j
            ckt = event.ckt
            bkey = str(ibus) + '-' + str(jbus) + '-' + ckt
            outage_dict['branch'].update({bkey: clabel})
        for event in con.generator_out_events:
            gbus = event.i
            gid = event.id
            gkey = str(gbus) + '-' + gid
            outage_dict['gen'].update({gkey: clabel})

    # -- GET GENERATOR PARTICIPATION FACTORS ------------------------------------------------------
    for pf_record in raw_data.inl.generator_inl_records.values():
        gkey = str(pf_record.i) + '-' + pf_record.id
        pfactor_dict.update({gkey: pf_record.r})

    # -- GET SWING BUS FROM RAW BUSDATA -----------------------------------------------------------
    swingbus, swing_kv, swing_angle, swing_vlow, swing_vhigh = get_swingbus_data(raw_data.raw.buses)

    # -- GET SWING GEN DATA FROM GENDATA (REMOVE SWING GEN FROM GENDATA) --------------------------
    swgens_data = get_swgens_data(swingbus, raw_data.raw.generators)

    # -- GET ALTERNATE SWING BUS ------------------------------------------------------------------
    swingbus1, sw1_genkey = get_alt_swingbus(raw_data.raw.generators, raw_data.raw.buses, swingbus, swing_kv, swgens_data[0][6])

    # =============================================================================================
    # == CREATE NETWORK ===========================================================================
    # =============================================================================================
    print('------------------------ CREATING NETWORKS -------------------------')
    create_starttime = time.time()
    kva_base = 1e3 * mva_base
    net_a = pp.create_empty_network('net_a', 60.0, kva_base)
    net_c = pp.create_empty_network('net_c', 60.0, kva_base)

    # == ADD BUSES TO NETWORK =====================================================================
    # buses = (bus.i, bus.ide, bus.baskv, bus.area, bus.vm, bus.va, bus.nvhi, bus.nvlo, bus.evhi, bus.evlo)
    print('ADD BUSES ..........................................................')
    busnomkvdict = {}
    buskvdict = {}
    busarea_dict = {}
    busidxs = []
    for bus in raw_data.raw.buses.values():
        busnum = bus.i
        busnomkv = bus.baskv
        busarea = bus.area
        buskv = bus.vm
        sw_vmax_a = bus.nvhi
        sw_vmin_a = bus.nvlo
        sw_vmax_c = bus.evhi
        sw_vmin_c = bus.evlo
        # -- BASE NETWORK -------------------------------------------------------------------------
        pp.create_bus(net_a, vn_kv=busnomkv, zone=busarea, max_vm_pu=sw_vmax_a, min_vm_pu=sw_vmin_a, index=busnum)
        # -- CONTINGENCY NETWORK ------------------------------------------------------------------
        idx = pp.create_bus(net_c, vn_kv=busnomkv, zone=busarea, max_vm_pu=sw_vmax_c, min_vm_pu=sw_vmin_c, index=busnum)
        if busnum == swingbus:
            swingbus_idx = idx
        busnomkvdict.update({busnum: busnomkv})
        buskvdict.update({busnum: buskv})
        busarea_dict.update({busnum: busarea})
        busidxs.append(idx)

    # == ADD LOADS TO NETWORK =====================================================================
    print('ADD LOADS ..........................................................')
    # loads = (load.i, load.id, load.status, load.pl, load.ql)
    for load in raw_data.raw.loads.values():
        status = bool(load.status)
        if not status:
            continue
        loadbus = load.i
        loadid = load.id
        loadname = str(loadbus) + '-' + loadid
        loadp = 1e3 * load.pl
        loadq = 1e3 * load.ql
        pp.create_load(net_a, bus=loadbus, p_kw=loadp, q_kvar=loadq, name=loadname)
        pp.create_load(net_c, bus=loadbus, p_kw=loadp, q_kvar=loadq, name=loadname)

    # == ADD GENERATORS TO NETWORK ================================================================
    print('ADD GENERATORS .....................................................')
    genbuses = []
    gids = []
    gendict = {}
    genidxdict = {}
    swinggen_idxs = []
    gen_status_vreg_dict = {}
    genbus_dict = {}
    genarea_dict = {}
    genidxs = []
    all_participating = True
    # -- ADD SWING GENERATORS ---------------------------------------------------------------------
    #  swgens_data = (key, id, pgen, qgen, qmax, qmin, vreg, pmax, pmin)
    for swgen_data in swgens_data:
        swing_kv = busnomkvdict[swingbus]
        genbus = swingbus
        genkey = swgen_data[0]
        gid = swgen_data[1]
        pgen = -1e3 * swgen_data[2]
        qgen = -1e3 * swgen_data[3]
        qmin = -1e3 * swgen_data[4]
        qmax = -1e3 * swgen_data[5]
        vreg = swgen_data[6]
        if GVREG:
            vreg = Gvreg_Custom
        pmin = -1e3 * swgen_data[7]
        pmax = -1e3 * swgen_data[8]
        gen_status_vreg_dict.update({genbus: [True, vreg]})
        pcostdata = None
        if genkey in gdisp_dict:
            disptablekey = gdisp_dict[genkey]
            costtablekey = pdisp_dict[disptablekey]
            pcostdata = numpy.array(pwlcost_dict[costtablekey])
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, name=genkey, min_p_kw=pmin, max_p_kw=pmax, min_q_kvar=qmin, max_q_kvar=qmax, controllable=True)
            pp.create_piecewise_linear_cost(net_a, idx, 'gen', pcostdata, type='p')
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, name=genkey, min_p_kw=pmin, max_p_kw=pmax, min_q_kvar=qmin, max_q_kvar=qmax, controllable=True, index=idx)
            pp.create_piecewise_linear_cost(net_c, idx, 'gen', pcostdata, type='p')
        else:
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, name=genkey, min_p_kw=pmin, max_p_kw=pmax, min_q_kvar=qmin, max_q_kvar=qmax, controllable=False)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, name=genkey, min_p_kw=pmin, max_p_kw=pmax, min_q_kvar=qmin, max_q_kvar=qmax, controllable=False, index=idx)
            all_participating = False
        swing_vreg = vreg
        swinggen_idxs.append(idx)
        gendict.update({genkey: idx})
        genidxdict.update({genbus: idx})
        genbuses.append(genbus)
        gids.append("'" + gid + "'")
        genbus_dict.update({genkey: genbus})
        genarea_dict.update({genkey: busarea_dict[genbus]})
        genidxs.append(idx)

    # -- ADD REMAINING GENERATOR ------------------------------------------------------------------
    # gens = (gen.i, gen.id, gen.pg, gen.qg, gen.qt, gen.qb, gen.vs, gen.pt, gen.pb, gen.stat)
    for gen in raw_data.raw.generators.values():
        genbus = gen.i
        gid = gen.id
        vreg = gen.vs
        if GVREG:
            vreg = Gvreg_Custom
        pgen = -1e3 * gen.pg
        qgen = -1e3 * gen.qg
        qmin = -1e3 * gen.qt
        qmax = -1e3 * gen.qb
        pmin = -1e3 * gen.pt
        pmax = -1e3 * gen.pb
        status = bool(gen.stat)
        gen_status_vreg_dict.update({genbus: [False, vreg]})
        pcostdata = None
        genkey = str(genbus) + '-' + str(gid)
        if genkey in gdisp_dict:
            disptablekey = gdisp_dict[genkey]
            costtablekey = pdisp_dict[disptablekey]
            pcostdata = numpy.array(pwlcost_dict[costtablekey])
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, name=genkey, min_p_kw=pmin, max_p_kw=pmax, min_q_kvar=qmin, max_q_kvar=qmax, controllable=True, in_service=status)
            # TODO may be non-convex cost curves
            if costtablekey not in []:
                pp.create_piecewise_linear_cost(net_a, idx, 'gen', pcostdata, type='p')
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, name=genkey, min_p_kw=pmin, max_p_kw=pmax, min_q_kvar=qmin, max_q_kvar=qmax, controllable=True, in_service=status, index=idx)
            # TODO may be non-convex cost curves
            if costtablekey not in []:
                pp.create_piecewise_linear_cost(net_c, idx, 'gen', pcostdata, type='p')
        else:
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, name=genkey, min_p_kw=pmin, max_p_kw=pmax, min_q_kvar=qmin, max_q_kvar=qmax, controllable=True, in_service=status)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, name=genkey, min_p_kw=pmin, max_p_kw=pmax, min_q_kvar=qmin, max_q_kvar=qmax, controllable=False, in_service=status, index=idx)
            all_participating = False
        if status:
            gen_status_vreg_dict[genbus][0] = status
        gids.append("'" + gid + "'")
        gendict.update({genkey: idx})
        genidxdict.update({genbus: idx})
        genbuses.append(genbus)
        genbus_dict.update({genkey: genbus})
        genarea_dict.update({genkey: busarea_dict[genbus]})
        genidxs.append(idx)

    # == ADD FIXED SHUNT DATA TO NETWORK ==========================================================
    # fixshunt = (fxshunt.i, fxshunt.id, fxshunt.status, fxshunt.gl, fxshunt.bl)
    fxidxdict = {}
    if raw_data.raw.fixed_shunts.values():
        print('ADD FIXED SHUNTS ...................................................')
        for fxshunt in raw_data.raw.fixed_shunts.values():
            status = bool(fxshunt.status)
            if not status:
                continue
            shuntbus = fxshunt.i
            shuntname = str(shuntbus) + '-FX'
            kw = -1e3 * fxshunt.gl
            kvar = -1e3 * fxshunt.bl
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_shunt(net_a, shuntbus, kvar, p_kw=kw, step=1, max_step=True, name=shuntname)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_shunt(net_c, shuntbus, kvar, p_kw=kw, step=1, max_step=True, name=shuntname, index=idx)
            fxidxdict.update({shuntbus: idx})

    # == ADD SWITCHED SHUNTS TO NETWORK ===========================================================
    # -- (SWSHUNTS ARE MODELED AS Q-GENERATORS) ---------------------------------------------------
    # swshunt = (swshunt.i, swshunt.binit, swshunt.n1, swshunt.b1, swshunt.n2, swshunt.b2, swshunt.n3, swshunt.b3, swshunt.n4, swshunt.b4,
    #            swshunt.n5, swshunt.b5, swshunt.n6, swshunt.b6, swshunt.n7, swshunt.b7, swshunt.n8, swshunt.b8, swshunt.stat)
    # gens = (gen.i, gen.id, gen.pg, gen.qg, gen.qt, gen.qb, gen.pt, gen.pb, gen.stat)
    swshidxdict = {}
    swshidxs = []
    if raw_data.raw.switched_shunts.values():
        print('ADD SWITCHED SHUNTS ................................................')
        for swshunt in raw_data.raw.switched_shunts.values():
            status = bool(swshunt.stat)
            if not status:
                continue
            shuntbus = swshunt.i
            vreg = buskvdict[shuntbus]
            if SWSHVREG:
                vreg = SwShVreg_Custom
            if shuntbus in gen_status_vreg_dict:
                if gen_status_vreg_dict[shuntbus][0]:
                    vreg = gen_status_vreg_dict[shuntbus][1]
            swshkey = str(shuntbus) + '-SW'
            steps = [swshunt.n1, swshunt.n2, swshunt.n3, swshunt.n4, swshunt.n5, swshunt.n6, swshunt.n7, swshunt.n8]
            kvars = [-1e3 * swshunt.b1, -1e3 * swshunt.b2, -1e3 * swshunt.b3, -1e3 * swshunt.b4, -1e3 * swshunt.b5, -1e3 * swshunt.b6, -1e3 * swshunt.b7, -1e3 * swshunt.b8]
            total_qmin = 0.0
            total_qmax = 0.0
            for j in range(len(kvars)):
                if kvars[j] < 0.0:
                    total_qmin += steps[j] * kvars[j]
                elif kvars[j] > 0.0:
                    total_qmax += steps[j] * kvars[j]
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, shuntbus, -0.0, vm_pu=vreg, min_q_kvar=total_qmin, max_q_kvar=total_qmax, min_p_kw=0.0, max_p_kw=0.0, controllable=True, name=swshkey, type='swsh')
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_gen(net_c, shuntbus, -0.0, vm_pu=vreg, min_q_kvar=total_qmin, max_q_kvar=total_qmax, min_p_kw=0.0, max_p_kw=0.0, controllable=True, name=swshkey, type='swsh', index=idx)
            swshidxdict.update({shuntbus: idx})
            swshidxs.append(idx)

    # == ADD LINES TO NETWORK =====================================================================
    # line = (line.i, line.j, line.ckt, line.r, line.x, line.b, line.ratea, line.ratec, line.st)
    linedict = {}
    line_ratea_dict = {}
    lineidxs = []
    print('ADD LINES ..........................................................')
    for line in raw_data.raw.nontransformer_branches.values():
        frombus = line.i
        tobus = line.j
        ckt = line.ckt
        status = bool(line.st)
        length = 1.0
        kv = busnomkvdict[frombus]
        zbase = kv ** 2 / mva_base
        r_pu = line.r
        x_pu = line.x
        b_pu = line.b
        r = r_pu * zbase
        x = x_pu * zbase
        b = b_pu / zbase
        capacitance = 1e9 * b / (2 * math.pi * 60.0)
        base_mva_rating = line.ratea
        mva_rating = line.ratec
        i_rating_a = base_mva_rating / (math.sqrt(3) * kv)
        i_rating_c = mva_rating / (math.sqrt(3) * kv)
        linekey = str(frombus) + '-' + str(tobus) + '-' + ckt
        # -- BASE NETWORK -------------------------------------------------------------------------
        idx = pp.create_line_from_parameters(net_a, frombus, tobus, length, r, x, capacitance, i_rating_a, name=linekey, max_loading_percent=100.0, in_service=status)
        # -- CONTINGENCY NETWORK ------------------------------------------------------------------
        pp.create_line_from_parameters(net_c, frombus, tobus, length, r, x, capacitance, i_rating_c, name=linekey, max_loading_percent=100.0, in_service=status, index=idx)
        linedict.update({linekey: idx})
        lineidxs.append(idx)

    # == ADD 2W TRANSFORMERS TO NETWORK ===========================================================
    # 2wxfmr = (xfmr.i, xfmr.j, xfmr.ckt, xfmr.mag1, xfmr.mag2, xfmr.r12, xfmr.x12, xfmr.windv1, xfmr.ang1, xfmr.rata1, xfmr.ratc1, xfmr.windv2, xfmr.stat)
    xfmrdict = {}
    xfmr_ratea_dict = {}
    xfmridxs = []
    print('ADD 2W TRANSFORMERS ................................................')
    for xfmr in raw_data.raw.transformers.values():
        status = bool(xfmr.stat)
        frombus = xfmr.i
        tobus = xfmr.j
        ckt = xfmr.ckt
        fromkv = busnomkvdict[frombus]
        tokv = busnomkvdict[tobus]
        if fromkv > tokv:                           # force from bus to be highside
            frombus, tobus = tobus, frombus
            fromkv, tokv = tokv, fromkv
        r_pu = xfmr.r12                             # @ mva_base
        x_pu = xfmr.x12                             # @ mva_base
        base_mva_rating = xfmr.rata1
        mva_rating = xfmr.ratc1
        ra_pu = r_pu * base_mva_rating / mva_base   # pandapower uses given transformer rating as test mva
        xa_pu = x_pu * base_mva_rating / mva_base   # so convert to mva_rating base
        za_pu = math.sqrt(ra_pu ** 2 + xa_pu ** 2)  # calculate 'nameplate' pu impedance
        za_pct = 100.0 * za_pu                      # pandadower uses percent impedance
        ra_pct = 100.0 * ra_pu                      # pandadower uses percent resistance
        kva_rating_a = 1e3 * base_mva_rating        # rate a for base case analysis
        rc_pu = r_pu * mva_rating / mva_base        # pandapower uses given transformer rating as test mva
        xc_pu = x_pu * mva_rating / mva_base        # so convert to mva_rating base
        zc_pu = math.sqrt(rc_pu ** 2 + xc_pu ** 2)  # calculate 'nameplate' pu impedance
        zc_pct = 100.0 * zc_pu                      # pandadower uses percent impedance
        rc_pct = 100.0 * rc_pu                      # pandadower uses percent resistance
        kva_rating_c = 1e3 * mva_rating             # rate c for contingency analysis
        noloadlosses = 0.0
        ironlosses = 0.0
        xfmr2wkey = str(frombus) + '-' + str(tobus) + '-' + ckt
        # -- BASE NETWORK -------------------------------------------------------------------------
        idx = pp.create_transformer_from_parameters(net_a, frombus, tobus, kva_rating_a, fromkv, tokv, ra_pct, za_pct, ironlosses, noloadlosses, shift_degree=0.0,
                                                    max_loading_percent=100.0, name=xfmr2wkey, in_service=status)
        # -- CONTINGENCY NETWORK ------------------------------------------------------------------
        pp.create_transformer_from_parameters(net_c, frombus, tobus, kva_rating_c, fromkv, tokv, rc_pct, zc_pct, ironlosses, noloadlosses, shift_degree=0.0,
                                              max_loading_percent=100.0, name=xfmr2wkey, index=idx, in_service=status)
        xfmrdict.update({xfmr2wkey: idx})
        xfmr_ratea_dict.update({xfmr2wkey: kva_rating_a})
        xfmridxs.append(idx)

    # == ADD EXTERNAL GRID ========================================================================
    # == WITH DUMMY TIE TO RAW SWING BUS ==========================================================
    # == DUMMY TIE RATING UP WITH LOWER KV ========================================================
    ext_tie_rating = 1e5/(math.sqrt(3) * swing_kv)

    # -- CREATE BASE NETWORK EXTERNAL GRID --------------------------------------------------------
    ext_grid_idx = pp.create_bus(net_a, vn_kv=swing_kv, name='Ex_Grid_Bus', max_vm_pu=sw_vmax_a, min_vm_pu=sw_vmin_a)
    tie_idx = pp.create_line_from_parameters(net_a, swingbus, ext_grid_idx, 1.0, 0.0, 0.001, 0.0, ext_tie_rating, name='Swing-Tie', max_loading_percent=100.0)
    pp.create_ext_grid(net_a, ext_grid_idx, vm_pu=swing_vreg, va_degree=swing_angle,   min_p_kw=-1e-9, max_p_kw=0.0,   min_q_kvar=0.0,  max_q_kvar=1e-9, index=ext_grid_idx)
    # pp.create_ext_grid(net_a, ext_grid_idx, vm_pu=swing_vreg, va_degree=swing_angle, min_p_kw=-1e-6, max_p_kw=0, min_q_kvar=0.0, max_q_kvar=1e-6, index=ext_grid_idx)
    pp.create_polynomial_cost(net_a, ext_grid_idx, 'ext_grid', numpy.array([-1, 1e6]), type='p')
    # pp.create_polynomial_cost(net_a, ext_grid_idx, 'ext_grid', numpy.array([-1, 1e6]), type='q')

    # -- CREATE CONTINGENCY NETWORK EXTERNAL GRID -------------------------------------------------
    pp.create_bus(net_c, vn_kv=swing_kv, name='Ex_Grid_Bus', max_vm_pu=sw_vmax_c, min_vm_pu=sw_vmin_c, index=ext_grid_idx)
    tie_idx = pp.create_line_from_parameters(net_c, swingbus, ext_grid_idx, 1.0, 0.0, 0.001, 0.0, ext_tie_rating, name='Swing-Tie', max_loading_percent=100.0)
    pp.create_ext_grid(net_c, ext_grid_idx, vm_pu=swing_vreg, va_degree=swing_angle,   min_p_kw=-1e-9, max_p_kw=0.0,   min_q_kvar=0.0,  max_q_kvar=1e-9, index=ext_grid_idx)
    # pp.create_ext_grid(net_c, ext_grid_idx, vm_pu=swing_vreg, va_degree=swing_angle,   min_p_kw=-1e-6, max_p_kw=1000,   min_q_kvar=-1000,  max_q_kvar=1e-6, index=ext_grid_idx)
    pp.create_polynomial_cost(net_c, ext_grid_idx, 'ext_grid', numpy.array([-1, 1e6]), type='p')
    # pp.create_polynomial_cost(net_c, ext_grid_idx, 'ext_grid', numpy.array([-1, 1e6]), type='q')
    print('---------------------- DONE CREATING NETWORKS ----------------------', round(time.time() - create_starttime, 3))

    # ---------------------------------------------------------------------------------------------
    # -- SOLVE INITIAL NETWORKS WITH STRAIGHT POWERFLOW -------------------------------------------
    # ---------------------------------------------------------------------------------------------
    solve_starttime = time.time()
    pp.runpp(net_a, enforce_q_lims=True)                                                            # SOLVE INITIAL BASE NETWORK
    pp.runpp(net_c, enforce_q_lims=True)                                                            # SOLVE INITIAL CONTINGENCY NETWORK
    print('INITIAL NETWORKS SOLVED ............................................', round(time.time() - solve_starttime, 3))

    swsh_q_mins = net_a.gen.loc[swshidxs, 'min_q_kvar']                                             # GET COPY OF SWSHUNT QMINS
    swsh_q_maxs = net_a.gen.loc[swshidxs, 'max_q_kvar']                                             # GET COPY OF SWSHUNT QMAXS
    pfactor_total = 0.0                                                                             # INITIALIZE FLOAT
    for gen_pkey in pfactor_dict:
        gidx = gendict[gen_pkey]
        if not net_a.gen.loc[gidx, 'in_service']:
            continue
        pfactor_total += pfactor_dict[gen_pkey]                                                      # INCREMENT PFACTOR TOTAL

    # =============================================================================================
    # -- ATTEMPT TO SET UP BASECASE FOR SCOPF -----------------------------------------------------
    # =============================================================================================
    scopf_starttime = time.time()
    print('------------- ATTEMPTING TO INITIALIZE BASECASE SCOPF --------------')
    net = copy.deepcopy(net_a)                                                                      # GET FRESH COPY OF BASECASE NETWORK
    
##    try:
##        lidx = linedict['87-141-1']                                   # HARDCODED CONTINGENCY
##        net.line.loc[lidx, 'in_service'] = False                                                        # SWITCH OUT OF SERVICE
##    except:
##        pass
    
    pp.runopp(net, enforce_q_lims=True)                                                             # RUN OPF ON THIS NETWORK
    pp.runopp(net_a, enforce_q_lims=True)                                                           # RUN OPF ON BASECASE NETWORK
    pp.runopp(net_c, enforce_q_lims=True)                                                           # RUN OPF ON CONTINGENCY NETWORK
    net.gen['p_kw'] = net.res_gen['p_kw']                                                           # SET THIS NETWORK GENERATORS POWER TO OPF GENERATORS POWER (WITH OUTAGE)
    net_a.gen['p_kw'] = net_a.res_gen['p_kw']                                                       # SET BASECASE NETWORK GENERATORS POWER TO OPF GENERATORS POWER (NO OUTAGE)
    net_c.gen['p_kw'] = net_c.res_gen['p_kw']                                                       # SET CONTINGENCY NETWORK GENERATORS POWER TO OPF GENERATORS POWER (NO OUTAGE)

    for genbus in genidxdict:                                                                       # LOOP ACROSS GENERATOR BUSES
        if genbus == swingbus:                                                                      # CHECK IF SWING SWING BUS
            continue
        gidx = genidxdict[genbus]                                                                   # GET GENERATOR INDEX
        net.gen.loc[gidx, 'vm_pu'] = net.res_bus.loc[genbus, 'vm_pu']                               # SET THIS NETWORK GENS VREG TO THIS NETWORK OPF GENS VREG
        net_a.gen.loc[gidx, 'vm_pu'] = net.res_bus.loc[genbus, 'vm_pu']                             # SET BASECASE NETWORK GENS VREG TO THIS NETWORK OPF GENS VREG
        net_c.gen.loc[gidx, 'vm_pu'] = net.res_bus.loc[genbus, 'vm_pu']                             # SET CONTINGENCY NETWORK GENS VREG TO THIS NETWORK OPF GENS VREG

    for shbus in swshidxdict:                                                                       # LOOP ACROSS SWSHUNT (GEN) BUSES
        shidx = swshidxdict[shbus]                                                                  # GET SWSHUNT INDEX
        net.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']                               # SET THIS NETWORK SWSHUNT VREG TO THIS NETWORK OPF SWSHUNT VREG
        net_a.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']                             # SET BASECASE NETWORK SWSHUNT VREG TO THIS NETWORK OPF SWSHUNT VREG
        net_c.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']                             # SET CONTINGENCY NETWORK SWSHUNT VREG TO THIS NETWORK OPF SWSHUNT VREG

    pp.runopp(net, enforce_q_lims=True)                                                             # RUN OPF ON THIS NETWORK
    pp.runopp(net_a, enforce_q_lims=True)                                                           # RUN OPF ON BASECASE NETWORK
    pp.runopp(net_c, enforce_q_lims=True)                                                           # RUN OPF ON CONTINGENCY NETWORK
    net.gen['p_kw'] = net.res_gen['p_kw']                                                           # SET THIS NETWORK GENERATORS POWER TO OPF GENERATORS POWER (WITH OUTAGE)
    net_a.gen['p_kw'] = net_a.res_gen['p_kw']                                                       # SET BASECASE NETWORK GENERATORS POWER TO OPF GENERATORS POWER (NO OUTAGE)
    net_c.gen['p_kw'] = net_c.res_gen['p_kw']                                                       # SET CONTINGENCY NETWORK GENERATORS POWER TO OPF GENERATORS POWER (NO OUTAGE)
    for genbus in genidxdict:                                                                       # LOOP ACROSS GENERATOR BUSES
        if genbus == swingbus:                                                                      # CHECK IF SWING SWING BUS
            continue
        gidx = genidxdict[genbus]                                                                   # GET GENERATOR INDEX
        net.gen.loc[gidx, 'vm_pu'] = net.res_bus.loc[genbus, 'vm_pu']                               # SET THIS NETWORK GENS VREG TO THIS NETWORK OPF GENS VREG
        net_a.gen.loc[gidx, 'vm_pu'] = net.res_bus.loc[genbus, 'vm_pu']                             # SET BASECASE NETWORK GENS VREG TO THIS NETWORK OPF GENS VREG
        net_c.gen.loc[gidx, 'vm_pu'] = net.res_bus.loc[genbus, 'vm_pu']                             # SET CONTINGENCY NETWORK GENS VREG TO THIS NETWORK OPF GENS VREG

    for shbus in swshidxdict:                                                                       # LOOP ACROSS SWSHUNT (GEN) BUSES
        shidx = swshidxdict[shbus]                                                                  # GET SWSHUNT INDEX
        net.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']                               # SET THIS NETWORK SWSHUNT VREG TO THIS NETWORK OPF SWSHUNT VREG
        net_a.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']                             # SET BASECASE NETWORK SWSHUNT VREG TO THIS NETWORK OPF SWSHUNT VREG
        net_c.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']                             # SET CONTINGENCY NETWORK SWSHUNT VREG TO THIS NETWORK OPF SWSHUNT VREG

    # -- SET SWING GENS AND EXTGRID VREG = AVERAGE OF BUS VOLTAGES NLEVELS OUT --------------------
    buses = [swingbus]                                                                              # INITIALIZE BUS LIST
    levels_out = 3                                                                                  # DEFINE NLEVELS
    for i in range(levels_out):                                                                     # LOOP THROUGH LEVELS OUT...
        buses = pp.get_connected_buses(net, buses, consider=('l', 't'), respect_in_service=True)    # GET BUSES FOR THIS LEVEL
    buses = [x for x in buses if x != (ext_grid_idx or swingbus)]                                   # ELIMINATE SWING AND EXTGRID BUSES
    buses_v = [x for x in net.res_bus.loc[buses, 'vm_pu']]                                          # GET LIST OF THE BUS VOLTAGES
    ave_v = sum(buses_v) / len(buses_v)                                                             # CALCULATE AVERAGE VOLTAGE
    net.gen.loc[swinggen_idxs, 'vm_pu'] = ave_v                                                     # SET SWING GENS VREG = AVE_V
    net_a.gen.loc[swinggen_idxs, 'vm_pu'] = ave_v                                                   # SET SWING GENS VREG = AVE_V
    net_c.gen.loc[swinggen_idxs, 'vm_pu'] = ave_v                                                   # SET SWING GENS VREG = AVE_V
    net.ext_grid.loc[ext_grid_idx, 'vm_pu'] = ave_v                                                 # SET EXTGRID VREG = AVE_V
    net_a.ext_grid.loc[ext_grid_idx, 'vm_pu'] = ave_v                                               # SET EXTGRID VREG = AVE_V
    net_c.ext_grid.loc[ext_grid_idx, 'vm_pu'] = ave_v                                               # SET EXTGRID VREG = AVE_V
    print('SWING VREG ESTIMATE = {0:.4f} .......................................'.format(ave_v))
    pp.runpp(net, enforce_q_lims=True)                                                              # RUN FINAL STRAIGHT POWER FLOW ON THIS NETWORK
    pp.runpp(net_a, enforce_q_lims=True)                                                            # RUN FINAL STRAIGHT POWER FLOW ON BASECASE NETWORK
    pp.runpp(net_c, enforce_q_lims=True)                                                            # RUN FINAL STRAIGHT POWER FLOW ON CONTINGENCY NETWORK

    # =============================================================================================
    # -- RUN RATEA BASECASE OPTIMAL POWER FLOW ----------------------------------------------------
    # =============================================================================================
    pp.runpp(net_a, enforce_q_lims=True)                                                            # RUN STRAIGHT POWER FLOW ON BASECASE
    # -- HACK FOR REPORTING SWITCHED SHUNT SUSCEPTANCE INSTEAD OF VARS --------
    for shbus in swshidxdict:                                                                       # LOOP ACROSS SWSHUNT (GEN) BUSES
        shidx = swshidxdict[shbus]                                                                  # GET SWSHUNT INDEX
        busv = net_a.res_bus.loc[shbus, 'vm_pu']                                                    # GET SWSHUNT BUS VOLTAGE
        kvar = net_a.res_gen.loc[shidx, 'q_kvar']                                                   # GET SWSHUNT VARS
        kvar_1pu = kvar / busv ** 2                                                                 # CALCULATE VARS SUSCEPTANCE
        if busv > 1.0:                                                                              # IF BUS VOLTAGE > 1.0 PU...
            net_a.gen.loc[shidx, 'min_q_kvar'] = kvar_1pu                                           # SET MIN SWSHUNT VARS
        elif busv < 1.0:                                                                            # IF BUS VOLTAGE < 1.0 PU...
            net_a.gen.loc[shidx, 'max_q_kvar'] = kvar_1pu                                           # SET MAX SWSHUNT VARS
    pp.runpp(net_a, enforce_q_lims=True)                                                            # RUN STRAIGHT POWER FLOW ON BASECASE
    net_a.gen.loc[swshidxs, 'min_q_kvar'] = swsh_q_mins                                             # RESTORE ORIGINAL SWSHUNT QMINS
    net_a.gen.loc[swshidxs, 'max_q_kvar'] = swsh_q_maxs                                             # RESTORE ORIGINAL SWSHUNT QMAXS
    pp.runpp(net_a, enforce_q_lims=True)                                                            # RUN STRAIGHT POWER FLOW ON BASECASE
    ex_pgen = net_a.res_ext_grid.loc[ext_grid_idx, 'p_kw']                                          # GET EX_GRID REAL POWER
    for idx in swinggen_idxs:                                                                       # LOOP ACROSS GENERATORS CONNECTED TO SWING BUS
        net_a.gen.loc[idx, 'p_kw'] += ex_pgen / len(swinggen_idxs)                                  # DISTRIBUTE EX_PGEN ACROSS SWING GENERATORS
    pp.runpp(net_a, enforce_q_lims=True)                                                            # RUN FINAL STRAIGHT POWER FLOW ON BASECASE

    # -- WRITE BASECASE BUS AND GENERATOR RESULTS TO FILE -----------------------------------------
    bus_results = copy.deepcopy(net_a.res_bus)                                                      # GET BASECASE BUS RESULTS
    gen_results = copy.deepcopy(net_a.res_gen)                                                      # GET BASECASE GENERATOR RESULTS
    write_base_bus_results(outfname1, bus_results, swshidxdict, gen_results, ext_grid_idx)          # WRITE SOLUTION1 BUS RESULTS
    write_base_gen_results(outfname1, gen_results, gids, genbuses, swshidxs)                        # WRITE SOLUTION1 GEN RESULTS
    print('BASECASE SCOPF INITIALIZED ............................................', round(time.time() - scopf_starttime, 3))

    # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
    # -- RUN GENERATOR OUTAGES TO FIND OPTIMUM DELTA VARIABLE -------------------------------------
    # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
    # -- TODO run outages and get delta in Mypython2 ----------------------------------------------
    # gdelta_starttime = time.time()
    # for genkey in outage_dict['gen']:                                                               # LOOP THROUGH GENERATOR OUTAGES
    #     if genkey not in gendict:                                                                   # CHECK IF GENERATOR EXISTS
    #         print('GENERATOR NOT FOUND ................................................', genkey)   # PRINT MESSAGE
    #         continue
    #     gnet = copy.deepcopy(basec_net)                                                             # INITIALIZE THIS CONTINGENCY NETWORK
    #     genidx = gendict[genkey]                                                                    # GET OUTAGED GENERATOR INDEX
    #     if not gnet.gen.loc[genidx, 'in_service']:                                                  # CHECK IF OUTAGED GENERATOR IS ONLINE
    #         continue
    #     conlabel = outage_dict['gen'][genkey]                                                       # GET CONTINGENCY LABEL
    #     gen_outage_p = net_a.gen.loc[genidx, 'p_kw']                                                 # GET OUTAGED GENERATOR PGEN
    #     gnet.gen.in_service[genidx] = False                                                         # SWITCH OFF OUTAGED GENERATOR
    #
    #     # -- CALCULATE PARTICIPATING GENERATORS UP MARGIN -----------------------------------------
    #     p_margin_total = 0.0                                                                        # INITIALIZE GENS TOTAL UP POWER MARGIN
    #     p_margin = {}                                                                               # INITIALIZE GENS POWER MARGIN DICT
    #     for gen_pkey in pfactor_dict:                                                               # LOOP THROUGH PARTICIPATING GENERATORS
    #         gidx = gendict[gen_pkey]                                                                # GET PARTICIPATING GENERATOR INDEX
    #         if not gnet.gen.loc[gidx, 'in_service']:                                                # CHECK IF PARTICIPATING GENERATOR IS ONLINE
    #             continue
    #         pgen_a = net_a.gen.loc[gidx, 'p_kw']                                                # THIS GENERATORS RATEA BASECASE PGEN
    #         pgen = gnet.gen.loc[gidx, 'p_kw']                                                       # THIS GENERATORS RATEC BASECASE PGEN
    #         pmin = gnet.gen.loc[gidx, 'min_p_kw']                                                   # THIS GENERATORS RATEC BASECASE PMIN
    #         margin = pmin - pgen                                                                    # THIS GENERATORS UP MARGIN
    #         p_margin.update({gidx: margin})                                                         # UPDATE GENS POWER MARGIN DICT
    #         p_margin_total += margin                                                                # INCREMENT UP POWER MARGIN TOTAL
    #
    #     # -- FIRST ESTIMATE OF DELTA VARIABLE -----------------------------------------------------
    #     pfactor_total = 0.0                                                                         # INITIALIZE FLOAT
    #     for gen_pkey in pfactor_dict:                                                               # LOOP THROUGH PARTICIPATING GENERATORS
    #         gidx = gendict[gen_pkey]                                                                # GET PARTICIPATING GENERATOR INDEX
    #         if not gnet.gen.loc[gidx, 'in_service']:                                                # CHECK IF PARTICIPATING GENERATOR IS ONLINE
    #             continue
    #         pfactor_total += pfactor_dict[gen_pkey]                                                 # INCREMENT PFACTOR TOTAL
    #         pgen_a = net_a.gen.loc[gidx, 'p_kw']                                                # THIS GENERATORS RATEA BASECASE PGEN
    #         pgen = gnet.gen.loc[gidx, 'p_kw']                                                       # THIS GENERATORS RATEC BASECASE PGEN
    #         delta_pgen = gen_outage_p * p_margin[gidx] / p_margin_total                              # CALCULATE THIS GENERATORS CHANGE (PROPORTIONAL TO MARGIN)
    #         gnet.gen.loc[gidx, 'p_kw'] = pgen_a + delta_pgen                                        # SET THIS GENERATORS PGEN
    #     pp.runpp(gnet, enforce_q_lims=True)                                                         # RUN STRAIGHT POWER FLOW
    #     ex_pgen = gnet.res_ext_grid.loc[ext_grid_idx, 'p_kw']                                       # GET EXTERNAL GRID POWER
    #     delta = (gen_outage_p + ex_pgen) / pfactor_total                                             # FIRST ESTIMATE OF DELTA VARIABLE
    #
    #     pp.runopp(gnet, enforce_q_lims=True)                                                        # RUN OPTIMAL POWER FLOW (OPTIMIZE SWSHUNTS)
    #     swsh_q_mins = gnet.gen.loc[swshidxs, 'min_q_kvar']                                          # GET COPY OF SWSHUNT QMINS
    #     swsh_q_maxs = gnet.gen.loc[swshidxs, 'max_q_kvar']                                          # GET COPY OF SWSHUNT QMAXS
    #     for shbus in swshidxdict:                                                                   # LOOP ACROSS SWSHUNT (GEN) BUSES
    #         shidx = swshidxdict[shbus]                                                              # GET SWSHUNT INDEX
    #         busv = gnet.res_bus.loc[shbus, 'vm_pu']                                                 # GET OPF SWSHUNT BUS VOLTAGE
    #         kvar = gnet.res_gen.loc[shidx, 'q_kvar']                                                # GET OPF VARS OF SWSHUNT
    #         kvar_1pu = kvar / busv ** 2                                                             # CALCULATE VARS SUSCEPTANCE
    #         if busv > 1.0:                                                                          # IF BUS VOLTAGE > 1.0 PU...
    #             gnet.gen.loc[shidx, 'min_q_kvar'] = kvar_1pu                                        # SET MIN SWSHUNT VARS
    #         elif busv < 1.0:                                                                        # IF BUS VOLTAGE < 1.0 PU...
    #             gnet.gen.loc[shidx, 'max_q_kvar'] = kvar_1pu                                        # SET MAX SWSHUNT VARS
    #     pp.runpp(gnet, enforce_q_lims=True)                                                         # RUN STRAIGHT POWER FLOW
    #     for shbus in swshidxdict:                                                                   # LOOP ACROSS SWSHUNT (GEN) BUSES
    #         shidx = swshidxdict[shbus]                                                              # GET SWSHUNT INDEX
    #         gnet.gen.loc[shidx, 'vm_pu'] = gnet.res_bus.loc[shbus, 'vm_pu']                         # SET SWSHUNT VREG TO OPF SWSHUNT BUS VOLTAGE
    #     pp.runpp(gnet, enforce_q_lims=True)                                                         # RUN STRAIGHT POWER FLOW
    #     gnet.gen.loc[swshidxs, 'min_q_kvar'] = swsh_q_mins                                          # RESTORE ORIGINAL SWSHUNT QMINS
    #     gnet.gen.loc[swshidxs, 'max_q_kvar'] = swsh_q_maxs                                          # RESTORE ORIGINAL SWSHUNT QMAXS
    #
    #     # -- ITERATE TO FIND OPTIMUM GENERATOR OUTAGE DELTA VARIABLE  ------------------------------
    #     step = 1
    #     while step < 120:                                                                           # LIMIT WHILE LOOPS
    #         net = copy.deepcopy(gnet)                                                               # GET FRESH COPY INITIALIZED NETWORK
    #         # net.gen.in_service[genidx] = False                                                    # SWITCH OFF OUTAGED GENERATOR
    #         for gen_pkey in pfactor_dict:                                                           # LOOP THROUGH PARTICIPATING GENERATORS
    #             gidx = gendict[gen_pkey]                                                            # GET THIS GENERATOR INDEX
    #             if not net.gen.loc[gidx, 'in_service']:                                             # CHECK IF GENERATOR IS ONLINE
    #                 continue
    #             pgen_a = net_a.gen.loc[gidx, 'p_kw']                                                # THIS GENERATORS RATEA BASECASE PGEN
    #             pmin = net.gen.loc[gidx, 'min_p_kw']                                                # THIS GENERATORS PMIN
    #             pmax = net.gen.loc[gidx, 'max_p_kw']                                                # THIS GENERATORS MAX
    #             pfactor = pfactor_dict[gen_pkey]                                                    # THIS GENERATORS PARTICIPATION FACTOR
    #             target_pgen = pgen_a + pfactor * delta                                              # CALCULATE THIS GENERATORS EXPECTED PGEN
    #             if pmin < target_pgen < pmax:                                                       # IF EXPECTED PGEN IS IN BOUNDS...
    #                 net.gen.loc[gidx, 'p_kw'] = target_pgen                                         # SET PGEN = EXPECTED PGEN
    #             elif target_pgen < pmin:                                                            # IF EXPECTED PGEN < PMIN...
    #                 net.gen.loc[gidx, 'p_kw'] = pmin                                                # SET PGEN = PMIN
    #             elif target_pgen > pmax:                                                            # IF EXPECTED PGEN > PMAX...
    #                 net.gen.loc[gidx, 'p_kw'] = pmax                                                # SET PGEN = PMAX
    #         # -- HACK FOR REPORTING SWITCHED SHUNT SUSCEPTANCE INSTEAD OF VARS ----
    #         for shbus in swshidxdict:                                                               # LOOP ACROSS SWSHUNT (GEN) BUSES
    #             shidx = swshidxdict[shbus]                                                          # GET SWSHUNT INDEX
    #             busv = net.res_bus.loc[shbus, 'vm_pu']                                              # GET OPF SWSHUNT BUS VOLTAGE
    #             kvar = net.res_gen.loc[shidx, 'q_kvar']                                             # GET OPF VARS OF SWSHUNT
    #             kvar_1pu = kvar / busv ** 2                                                         # CALCULATE VARS SUSCEPTANCE
    #             if busv > 1.0:                                                                      # IF BUS VOLTAGE > 1.0 PU...
    #                 net.gen.loc[shidx, 'min_q_kvar'] = kvar_1pu                                     # SET MIN SWSHUNT VARS
    #             elif busv < 1.0:                                                                    # IF BUS VOLTAGE < 1.0 PU...
    #                 net.gen.loc[shidx, 'max_q_kvar'] = kvar_1pu                                     # SET MAX SWSHUNT VARS
    #         pp.runpp(net, enforce_q_lims=True)                                                      # RUN STRAIGHT POWER FLOW
    #         ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_kw']                                    # GET EXTERNAL GRID POWER
    #         delta += ex_pgen / pfactor_total                                                        # INCREMENT DELTA
    #         if abs(ex_pgen) < 1.0:                                                                  # IF EXTERNAL GRID POWER IS NEAR ZERO..
    #             break                                                                               # BREAK AND GET NEXT GEN OUTAGE
    #         step += 1                                                                               # INCREMENT ITERATION
    #     ex_qgen = net.res_ext_grid.loc[ext_grid_idx, 'q_kvar']
    #     print('GEN {0:5s} . . . . . . . . MyPython1 TESTING  . . . . . . . . . . . .'.format(genkey),
    #           '\u0394 =', round(-1e-3 * delta, 5), '(' + str(step) + ')', round(ex_pgen, 3), round(ex_qgen + 0.0, 6))
    # if outage_dict['gen']:
    #     print('DELTAS FOR GENERATOR OUTAGES ESTIMATED .............................', round(time.time() - gdelta_starttime, 3))
    #
    # # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
    # # -- RUN LINE AND XFMR OUTAGES TO FIND OPTIMUM DELTA VARIABLE ---------------------------------
    # # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
    # # -- TODO run outages and get delta in Mypython2 ----------------------------------------------
    # bdelta_starttime = time.time()
    #
    # pfactor_total = 0.0                                                                        # INITIALIZE FLOAT
    # for gen_pkey in pfactor_dict:
    #     gidx = gendict[gen_pkey]
    #     if not net_a.gen.loc[gidx, 'in_service']:
    #         continue
    #     pfactor_total += pfactor_dict[gen_pkey]                                                # INCREMENT PFACTOR TOTAL

    # for branchkey in outage_dict['branch']:                                                         # LOOP THROUGH BRANCH OUTAGES
    #     if branchkey not in linedict and branchkey not in xfmrdict:                                 # CHECK IF BRANCH EXISTS...
    #         print('LINE OR TRANSFORMER NOT FOUND ......................................', branchkey)
    #         continue
    #     bnet = copy.deepcopy(basec_net)                                                             # INITIALIZE THIS CONTINGENCY NETWORK
    #     conlabel = outage_dict['branch'][branchkey]                                                 # GET CONTINGENCY LABEL
    #     if branchkey in linedict:                                                                   # CHECK IF BRANCH IS A LINE...
    #         lineidx = linedict[branchkey]                                                           # GET LINE INDEX
    #         if not bnet.line.loc[lineidx, 'in_service']:                                            # CHECK IF OUTAGED LINE IS IN-SERVICE...
    #             continue
    #         bnet.line.in_service[lineidx] = False                                                   # TAKE LINE OUT OF SERVICE
    #     elif branchkey in xfmrdict:                                                                 # CHECK IF BRANCH IS A XFMR...
    #         xfmridx = xfmrdict[branchkey]                                                           # GET XFMR INDEX
    #         if not bnet.trafo.loc[xfmridx, 'in_service']:                                           # CHECK IF OUTAGED XFMR IS IN-SERVICE...
    #             continue
    #         bnet.trafo.in_service[xfmridx] = False                                                  # TAKE XFMR OUT OF SERVICE
    #
    #     try:
    #         pp.runpp(bnet, enforce_q_lims=True)  # run straight power flow
    #     except:
    #         print('BRANCH {0:9s} . . . . . . . . MyPython1 TESTING . . . . . . . . .'.format(branchkey), 'DID NOT SOLVE WITH Q-LIMITS ENFORCED')
    #         continue
    #
    #     swsh_q_mins = bnet.gen.loc[swshidxs, 'min_q_kvar']                                          # GET COPY OF SWSHUNT QMINS
    #     swsh_q_maxs = bnet.gen.loc[swshidxs, 'max_q_kvar']                                          # GET COPY OF SWSHUNT QMAXS
    #     for shbus in swshidxdict:                                                                   # LOOP ACROSS SWSHUNT (GEN) BUSES
    #         shidx = swshidxdict[shbus]                                                              # GET SWSHUNT INDEX
    #         busv = bnet.res_bus.loc[shbus, 'vm_pu']                                                 # GET OPF SWSHUNT BUS VOLTAGE
    #         kvar = bnet.res_gen.loc[shidx, 'q_kvar']                                                # GET OPF VARS OF SWSHUNT
    #         kvar_1pu = kvar / busv ** 2                                                             # CALCULATE VARS SUSCEPTANCE
    #         if busv > 1.0:                                                                          # IF BUS VOLTAGE > 1.0 PU...
    #             bnet.gen.loc[shidx, 'min_q_kvar'] = kvar_1pu                                        # SET MIN SWSHUNT VARS
    #         elif busv < 1.0:                                                                        # IF BUS VOLTAGE < 1.0 PU...
    #             bnet.gen.loc[shidx, 'max_q_kvar'] = kvar_1pu                                        # SET MAX SWSHUNT VARS
    #
    #     try:
    #         pp.runpp(bnet, enforce_q_lims=True)  # run straight power flow
    #     except:
    #         print('BRANCH {0:9s} . . . . . . . . MyPython1 TESTING . . . . . . . . .'.format(branchkey), 'DID NOT SOLVE WITH Q-LIMITS ENFORCED')
    #         continue
    #     for shbus in swshidxdict:                                                                   # LOOP ACROSS SWSHUNT (GEN) BUSES
    #         shidx = swshidxdict[shbus]                                                              # GET SWSHUNT INDEX
    #         bnet.gen.loc[shidx, 'vm_pu'] = bnet.res_bus.loc[shbus, 'vm_pu']                         # SET SWSHUNT VREG TO OPF SWSHUNT BUS VOLTAGE
    #
    #
    #     try:
    #         pp.runpp(bnet, enforce_q_lims=True)  # run straight power flow
    #     except:
    #         print('BRANCH {0:9s} . . . . . . . . MyPython1 TESTING . . . . . . . . .'.format(branchkey), 'DID NOT SOLVE WITH Q-LIMITS ENFORCED')
    #         continue
    #
    #     bnet.gen.loc[swshidxs, 'min_q_kvar'] = swsh_q_mins                                          # RESTORE ORIGINAL SWSHUNT QMINS
    #     bnet.gen.loc[swshidxs, 'max_q_kvar'] = swsh_q_maxs                                          # RESTORE ORIGINAL SWSHUNT QMAXS
    #
    #     # -- ITERATE TO FIND OPTIMUM BRANCH OUTAGE DELTA VARIABLE  ---------------------------------
    #     delta = 0.0
    #     step = 1
    #     while step < 120:                                                                            # LIMIT WHILE LOOPS
    #         net = copy.deepcopy(bnet)                                                               # GET FRESH COPY INITIALIZED NETWORK
    #         for gen_pkey in pfactor_dict:                                                           # LOOP THROUGH PARTICIPATING GENERATORS
    #             gidx = gendict[gen_pkey]                                                            # GET THIS GENERATOR INDEX
    #             if not net.gen.loc[gidx, 'in_service']:                                             # CHECK IF GENERATOR IS ONLINE
    #                 continue
    #             pgen_a = net_a.res_gen.loc[gidx, 'p_kw']                                            # THIS GENERATORS RATEA BASECASE PGEN
    #             pmin = net.gen.loc[gidx, 'min_p_kw']                                                # THIS GENERATORS PMIN
    #             pmax = net.gen.loc[gidx, 'max_p_kw']                                                # THIS GENERATORS MAX
    #             pfactor = pfactor_dict[gen_pkey]                                                    # THIS GENERATORS PARTICIPATION FACTOR
    #             target_pgen = pgen_a + pfactor * delta                                              # CALCULATE THIS GENERATORS EXPECTED PGEN
    #             if pmin < target_pgen < pmax:                                                       # IF EXPECTED PGEN IS IN BOUNDS...
    #                 net.gen.loc[gidx, 'p_kw'] = target_pgen                                         # SET PGEN = EXPECTED PGEN
    #             elif target_pgen < pmin:                                                            # IF EXPECTED PGEN < PMIN...
    #                 net.gen.loc[gidx, 'p_kw'] = pmin                                                # SET PGEN = PMIN
    #             elif target_pgen > pmax:                                                            # IF EXPECTED PGEN > PMAX...
    #                 net.gen.loc[gidx, 'p_kw'] = pmax                                                # SET PGEN = PMAX
    #         # -- HACK FOR REPORTING SWITCHED SHUNT SUSCEPTANCE INSTEAD OF VARS ----
    #         for shbus in swshidxdict:                                                               # LOOP ACROSS SWSHUNT (GEN) BUSES
    #             shidx = swshidxdict[shbus]                                                          # GET SWSHUNT INDEX
    #             busv = net.res_bus.loc[shbus, 'vm_pu']                                              # GET OPF SWSHUNT BUS VOLTAGE
    #             kvar = net.res_gen.loc[shidx, 'q_kvar']                                             # GET OPF VARS OF SWSHUNT
    #             kvar_1pu = kvar / busv ** 2                                                         # CALCULATE VARS SUSCEPTANCE
    #             if busv > 1.0:                                                                      # IF BUS VOLTAGE > 1.0 PU...
    #                 net.gen.loc[shidx, 'min_q_kvar'] = kvar_1pu                                     # SET MIN SWSHUNT VARS
    #             elif busv < 1.0:                                                                    # IF BUS VOLTAGE < 1.0 PU...
    #                 net.gen.loc[shidx, 'max_q_kvar'] = kvar_1pu                                     # SET MAX SWSHUNT VARS
    #
    #         pp.runpp(net, enforce_q_lims=True)                                                  # RUN STRAIGHT POWER FLOW
    #         ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_kw']  # GET EXTERNAL GRID POWER
    #         if abs(ex_pgen) < 1.0:
    #             break
    #         delta += ex_pgen / pfactor_total
    #         step += 1                                                                               # INCREMENT ITERATION
    #
    #     ex_qgen = net.res_ext_grid.loc[ext_grid_idx, 'q_kvar']
    #     print('BRANCH {0:9s} . . . . . . . . MyPython1 TESTING . . . . . . . . .'.format(branchkey),
    #           '\u0394 =', round(-1e-3 * delta, 5), '(' + str(step) + ')', round(ex_pgen, 3), round(ex_qgen + 0.0, 6))
    #
    # if outage_dict['branch']:
    #     print('DELTAS FOR LINE AND XFMR OUTAGES ESTIMATED .........................', round(time.time() - bdelta_starttime, 3))

    # =============================================================================================
    # -- WRITE DATA TO FILE -----------------------------------------------------------------------
    # =============================================================================================
    print('====================================================================')
    write_starttime = time.time()
    neta_fname = cwd + r'/sandbox/Network_01-10O/scenario_1/neta.p'
    netc_fname = cwd + r'/sandbox/Network_01-10O/scenario_1/netc.p'
    data_fname = cwd + r'/sandbox/Network_01-10O/scenario_1/netdata.pkl'
    margin_fname = cwd + r'/sandbox/Network_01-10O/scenario_1/margins.pkl'
    # -- WRITE RATEA NETWORK TO FILE --------------------------------------------------------------
    pp.to_pickle(net_a, neta_fname)
    # -- WRITE RATEC NETWORK TO FILE --------------------------------------------------------------
    pp.to_pickle(net_c, netc_fname)
    # -- WRITE DATA TO FILE -----------------------------------------------------------------------
    PFile = open(data_fname, 'wb')
    pickle.dump([outage_dict, gendict, xfmrdict, pfactor_dict, ext_grid_idx, gids, genbuses, swshidxs, swshidxdict, linedict, xfmrdict, genidxdict, swinggen_idxs], PFile)
    PFile.close()
    print('WRITING DATA TO FILE -----------------------------------------------', round(time.time() - write_starttime, 3))
    print('DONE ---------------------------------------------------------------')
    print('TOTAL TIME -------------------------------------------------------->', round(time.time() - start_time, 3))
