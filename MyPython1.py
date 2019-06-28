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

# -----------------------------------------------------------------------------
# -- USING COMMAND LINE -------------------------------------------------------
# -----------------------------------------------------------------------------
if sys.argv[1:]:
    print()
    con_fname = sys.argv[1]
    inl_fname = sys.argv[2]
    raw_fname = sys.argv[3]
    rop_fname = sys.argv[4]
    outfname1 = 'solution1.txt'
    outfname2 = 'solution2.txt'
    # neta_fname = os.path.join('submission', 'neta.p')
    # netc_fname = os.path.join('submission', 'netc.p')
    # data_fname = os.path.join('submission', 'netdata.pkl')
    # neta_fname = os.path.abspath('..') + r'/neta.p'
    # netc_fname = os.path.abspath('..') + r'/netc.p'
    # data_fname = os.path.abspath('..') + r'/netdata.pkl'

# -----------------------------------------------------------------------------
# -- DEVELOPMENT --- DEVELOPMENT --- DEVELOPMENT --- DEVELOPMENT --------------
# -----------------------------------------------------------------------------
if not sys.argv[1:]:
    con_fname = cwd + r'/Network_01R-10/scenario_1/case.con'
    inl_fname = cwd + r'/Network_01R-10/case.inl'
    raw_fname = cwd + r'/Network_01R-10/scenario_1/case.raw'
    rop_fname = cwd + r'/Network_01R-10/case.rop'
    outfname1 = cwd + r'/solution1.txt'
    # neta_fname = cwd + r'/Network_01R-10/scenario_1/neta.p'
    # netc_fname = cwd + r'/Network_01R-10/scenario_1/netc.p'
    # data_fname = cwd + r'/Network_01R-10/scenario_1/netdata.pkl'

    try:
        os.remove(outfname1)
    except FileNotFoundError:
        pass

SWVREG = 0                  # SWING GENERATORS VOLTAGE SCHEDULE ... 0=DEFAULT_GENV_RAW, 1=CUSTOM)
SwVreg_Custom = 1.040

GVREG = 0                   # NON-SWING GENERATORS VOLTAGE SCHEDULES ... 0=DEFAULT_GENV_RAW, 1=CUSTOM(ALL)
Gvreg_Custom = 1.03

SWSHVREG = 0                # SWITCHED SHUNTS VOLTAGE SCHEDULES ........ 0=DEFAULT_RAW, 1=CUSTOM(ALL)
SwShVreg_Custom = 1.03

MaxLoading = 99.99
# SWVREG = 0
# SwVreg_Custom = 1.040       # INITIAL SWING GENERATORS VOLTAGE SETPOINT (THEN CALCULATE FOR VAR MARGIN)


# =============================================================================
# -- FUNCTIONS ----------------------------------------------------------------
# =============================================================================
def read_data(raw_name, rop_name, inl_name, con_name):
    p = data_read.Data()
    # p = Data()
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
            swgens_data.append([gkey, str(gen.id), float(gen.pg), float(gen.qg), float(gen.qt), float(gen.qb), float(gen.vs), float(gen.pt), float(gen.pb), int(gen.stat)])
            del_gens.append((gen.i, gen.id))
    for gen in del_gens:
        del generators[gen]
    return swgens_data


def get_alt_swingbus(generators, buses, swbus, sw_kv, sw_reg):
    # generators = (gen.i, gen.id, gen.pg, gen.qg, gen.qt, gen.qb, gen.vs, gen.pt, gen.pb, gen.stat)
    # buses = (bus.i, bus.ide, bus.baskv, bus.area, bus.vm, bus.va, bus.nvhi, bus.nvlo, bus.evhi, bus.evlo)
    swbus1 = None
    swgkey = None
    max_pmargin_list = [[g.pt - g.pg, g.i, str(g.i) + '-' + g.id] for g in generators.values() if g.i != swbus and g.stat != 0]
    max_pmargin_list.sort(reverse=True)
    for g in range(len(max_pmargin_list)):
        gbus = max_pmargin_list[g][1]
        for bus in buses.values():
            if bus.i == gbus:
                max_pmargin_list[g].append(bus.baskv)
    sw_candidates = [g for g in max_pmargin_list if g[3] == sw_kv]
    sw_candidates.sort(reverse=True)
    swbus1 = sw_candidates[0][1]
    swgkey = sw_candidates[0][2]
    return swbus1, swgkey


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
        del b_results['p_mw']        # not used for reporting
    except KeyError:
        pass
    try:
        del b_results['q_mvar']      # not used for reporting
    except KeyError:
        pass
    # -- REMOVE EXTERNAL GRID BUS RESULTS -------------------------------------
    b_results.drop([exgridbus], inplace=True)
    # -- ADD BUSNUMBER COLUMN -------------------------------------------------
    b_results.insert(0, 'bus', b_results.index)
    # -- ADD SHUNT MVARS COLUMN (FILLED WITH 0.0) -----------------------------
    b_results['shunt_b'] = 0.0
    # -- RENAME COLUMN HEADINGS -----------------------------------------------
    b_results.rename(columns={'vm_pu': 'voltage_pu', 'va_degree': 'angle'}, inplace=True)
    # -- PREVENT NEGATIVE ZEROS -----------------------------------------------
    b_results['voltage_pu'] += 0.0
    b_results['angle'] += 0.0
    # -- CONVERT PANDAS DATAFRAME TO LIST FOR REPORTING -----------------------
    buslist = [b_results.columns.values.tolist()] + b_results.values.tolist()
    # -- GET ANY SHUNT MVARS FOR REPORTING ------------------------------------
    for j in range(1, len(buslist)):
        buslist[j][0] = int(buslist[j][0])
        bus_ = buslist[j][0]
        if bus_ in sw_dict:
            mvar_ = g_results.loc[sw_dict[bus_], 'q_mvar'] / b_results.loc[bus_, 'voltage_pu'] ** 2
            buslist[j][3] = mvar_ + 0.0
    # -- WRITE THE BUS RESULTS TO FILE ----------------------------------------
    write_csvdata(fname, buslist, [['--bus section']])
    return


def write_base_gen_results(fname, g_results, genids, gbuses, swsh_idxs):
    g_results.drop(swsh_idxs, inplace=True)
    del g_results['vm_pu']
    del g_results['va_degree']
    g_results['p_mw'] += 0.0
    g_results['q_mvar'] += 0.0
    g_results['p_mw'] += 0.0
    g_results['q_mvar'] += 0.0
    # -- RENAME COLUMN HEADINGS -----------------------------------------------
    g_results.rename(columns={'p_mw': 'mw', 'q_mvar': 'mvar'}, inplace=True)
    # -- ADD GENERATOR BUSNUMBERS AND IDS -------------------------------------
    g_results.insert(0, 'id', genids)
    g_results.insert(0, 'bus', gbuses)
    # -- CONVERT PANDAS DATAFRAME TO LIST FOR REPORTING -----------------------
    glist = [g_results.columns.values.tolist()] + g_results.values.tolist()
    # -- WRITE THE GENERATION RESULTS TO FILE ---------------------------------
    write_csvdata(fname, glist, [['--generator section']])
    return


def write_bus_results(fname, b_results, sw_dict, g_results, exgridbus, clabel):
    # -- DELETE UNUSED DATAFRAME COLUMNS --------------------------------------
    try:
        del b_results['p_mw']        # not used for reporting
    except KeyError:
        pass
    try:
        del b_results['q_mvar']      # not used for reporting
    except KeyError:
        pass
    # -- REMOVE EXTERNAL GRID BUS RESULTS -------------------------------------
    b_results.drop([exgridbus], inplace=True)
    # -- ADD BUSNUMBER COLUMN -------------------------------------------------
    b_results.insert(0, 'bus', b_results.index)
    # -- ADD SHUNT MVARS COLUMN (FILLED WITH 0.0) -----------------------------
    b_results['shunt_b'] = 0.0
    # -- RENAME COLUMN HEADINGS -----------------------------------------------
    b_results.rename(columns={'vm_pu': 'voltage_pu', 'va_degree': 'angle'}, inplace=True)
    # -- PREVENT NEGATIVE ZEROS -----------------------------------------------
    b_results['voltage_pu'] += 0.0
    b_results['angle'] += 0.0
    # -- CONVERT PANDAS DATAFRAME TO LIST FOR REPORTING -----------------------
    buslist = [b_results.columns.values.tolist()] + b_results.values.tolist()
    # -- GET ANY SHUNT MVARS FOR REPORTING ------------------------------------
    for j in range(1, len(buslist)):
        buslist[j][0] = int(buslist[j][0])
        bus_ = buslist[j][0]
        if bus_ in sw_dict:
            mvar_ = g_results.loc[sw_dict[bus_], 'q_mvar'] / (b_results.loc[bus_, 'voltage_pu'] ** 2)
            buslist[j][3] = mvar_ + 0.0

    # -- WRITE THE BUS RESULTS TO FILE ----------------------------------------
    write_csvdata(fname, [], [['--contingency'], ['label'], [clabel]])
    write_csvdata(fname, buslist, [['--bus section']])
    return


def write_gen_results(fname, g_results, genids, gbuses, delta, swsh_idxs):
    g_results.drop(swsh_idxs, inplace=True)
    del g_results['vm_pu']
    del g_results['va_degree']
    g_results['p_mw'] += 0.0
    g_results['q_mvar'] += 0.0
    g_results['p_mw'] += 0.0
    g_results['q_mvar'] += 0.0
    # -- RENAME COLUMN HEADINGS -----------------------------------------------
    g_results.rename(columns={'p_mw': 'mw', 'q_mvar': 'mvar'}, inplace=True)
    # -- ADD GENERATOR BUSNUMBERS AND IDS -------------------------------------
    g_results.insert(0, 'id', genids)
    g_results.insert(0, 'bus', gbuses)
    # -- CONVERT PANDAS DATAFRAME TO LIST FOR REPORTING -----------------------
    glist = [g_results.columns.values.tolist()] + g_results.values.tolist()
    # -- WRITE THE GENERATION RESULTS TO FILE ---------------------------------
    write_csvdata(fname, glist, [['--generator section']])
    write_csvdata(fname, [], [['--delta section'], ['delta_p'], [delta]])
    return


# def adj_swshunt_susceptance(_net, _swshidx_dict, _swsh_q_mins, _swsh_q_maxs):
#     for _shbus in _swshidx_dict:                                                    # loop across swshunt (gen) buses
#         _shidx = _swshidx_dict[_shbus]                                              # get swshunt index
#         _busv = _net.res_bus.loc[_shbus, 'vm_pu']                                   # get swshunt bus voltage
#         _maxq = _swsh_q_maxs[_shidx]
#         _mvar = _net.res_gen.loc[_shidx, 'q_mvar']                                  # get swshunt vars
#         if _busv > 1.0:                                                                              # IF BUS VOLTAGE > 1.0 PU...
#             _next_mvar = _maxq * _busv ** 2
#             _net.gen.loc[_shidx, 'max_q_mvar'] = _next_mvar                                          # SET MIN SWSHUNT VARS
#         elif _busv < 1.0:                                                                            # IF BUS VOLTAGE < 1.0 PU...
#             _next_mvar = _maxq * _busv ** 2
#             _net.gen.loc[_shidx, 'min_q_mvar'] = _next_mvar                                          # SET MAX SWSHUNT VARS
#         # print(_busv > 1.0, _shbus, _busv, _mvar, _next_mvar, _maxq)
#     return _net


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


def get_generation_cost(_net, _participating_gens, _gen_dict, _pwlcost_dict0):
    _cost = 0.0
    for _gkey in _participating_gens:
        _gidx = _gen_dict[_gkey]
        _pcostdata = _pwlcost_dict0[_gkey]
        _g_mw = _net.res_gen.loc[_gidx, 'p_mw']
        _xlist, _ylist = zip(*_pcostdata)
        _cost += numpy.interp(_g_mw, _xlist, _ylist)
    return _cost


def get_maxloading(net):
    line_loading = max(net.res_line['loading_percent'].values)                                      # get max line loading
    xfmr_loading = max(net.res_trafo['loading_percent'].values)                                     # get max xfmr loading
    max_loading = max(line_loading, xfmr_loading)                                                   # get max of maxs
    max_loading = round(max_loading, 3)
    return max_loading


def get_minmax_voltage(net, busdict):
    min_voltage = 9.99
    max_voltage = 0.00
    for bus in busdict:
        kv_nom, vlow, vhigh = busdict[bus]
        pu_voltage = net.res_bus.loc[bus, 'vm_pu']
        kv_voltage = pu_voltage * kv_nom
        if pu_voltage < vlow and pu_voltage < min_voltage:
            min_voltage = pu_voltage
        if pu_voltage > vhigh and pu_voltage > max_voltage:
            max_voltage = pu_voltage
    return min_voltage, max_voltage


def get_nosolves(ns_net, g_outagekeys, b_outagekeys, gendict, linedict, xfmrdict, step):
    not_solved = False
    solvedoutages = []                                                                              # initialize solved outages list
    nosolveoutages = []                                                                             # initialize nosolve outages list
    for g_key in g_outagekeys:                                                                      # loop through generator outages
        net = copy.deepcopy(ns_net)                                                                 # get fresh copy of nosolve network
        g_idx = gendict[g_key]                                                                      # get generator index
        net.gen.in_service[g_idx] = False                                                           # switch off outaged generator
        try:                                                                                        # try straight powerflow solution
            pp.runpp(net, enforce_q_lims=True)                                                      # run powerflow
            solvedoutages.append(g_key)                                                             # append solved outages list
        except:                                                                                     # if no solution...
            not_solved = True                                                                       # set nosovlve flag
            nosolveoutages.append(g_key)                                                            # append not solved outages list
    for b_key in b_outagekeys:                                                                      # loop through branch outages
        net = copy.deepcopy(ns_net)                                                                 # get fresh copy of nosolve network
        if b_key in linedict:                                                                       # check if branch is a line...
            line_idx = linedict[b_key]                                                              # get line index
            net.line.in_service[line_idx] = False                                                   # switch out outaged line
        elif b_key in xfmrdict:                                                                     # check if branch is a xfmr...
            xfmr_idx = xfmrdict[b_key]                                                              # get xfmr index
            net.trafo.in_service[xfmr_idx] = False                                                  # switch out outaged xfmr
        try:                                                                                        # try straight powerflow solution
            pp.runpp(net, init='results', enforce_q_lims=True)                                      # run powerflow
            solvedoutages.append(b_key)                                                             # append no solve outages list
        except:                                                                                     # if no solution...
            not_solved = True                                                                       # set nosovlve flag
            nosolveoutages.append(b_key)                                                            # append not solved outages list
    if not_solved:
        nstext = '{0:<2d} NOSOLVES'.format(len(nosolveoutages))
        print('PASS{0:2d} HAS NOSOLVE CONTINGENCIES ...................................'.format(step), nstext)
    return not_solved, solvedoutages, nosolveoutages


def get_voltage_contrained_outages(c_net, solvedoutages, onlinegens, gendict, linedict, xfmrdict, busdict, step):
    has_voltage_constraint = False
    voltage_constrained_outages = []
    volt_penalty = 0.0
    otext = ''                                                                                      # initialize outage text
    for o_key in solvedoutages:                                                                     # loop across solved contingencies
        net = copy.deepcopy(c_net)                                                                  # get fresh copy of network
        if o_key in onlinegens:                                                                     # check if a generator...
            otext = 'GEN '                                                                          # assign text
            g_idx = gendict[o_key]                                                                  # get generator index
            net.gen.in_service[g_idx] = False                                                       # switch off outaged generator
        elif o_key in linedict:                                                                     # check if a line...
            otext = 'LINE'                                                                          # assign text
            line_idx = linedict[o_key]                                                              # get line index
            net.line.in_service[line_idx] = False                                                   # switch out outaged line
        elif o_key in xfmrdict:                                                                     # check if a xfmr...
            otext = 'XFMR'                                                                          # assign text
            xfmr_idx = xfmrdict[o_key]                                                              # get xfmr index
            net.trafo.in_service[xfmr_idx] = False                                                  # switch out outaged xfmr
        try:                                                                                        # try straight powerflow solution
            pp.runpp(net, enforce_q_lims=True)                                                      # run powerflow
            min_busvoltage, max_busvoltage = get_minmax_voltage(net, busdict)
            if min_busvoltage < 9.99:
                has_voltage_constraint = True                                                       # set constraint flag
                voltage_constrained_outages.append([abs(1.0 - min_busvoltage), o_key])              # add outage to constrained outages list
                print('min bus voltage =', min_busvoltage)

            if max_busvoltage > 0.00:
                has_voltage_constraint = True                                                       # set constraint flag
                voltage_constrained_outages.append([abs(1.0 - max_busvoltage), o_key])              # add outage to constrained outages list
        except:                                                                                     # if no solution...
            print(otext, '{0:9s} NOT SOLVED USING INITIAL SCOPF BASECASE .............'.format(o_key))

    voltage_constrained_outages.sort(reverse=True)                                                  # sort constrained outages list
    for c in voltage_constrained_outages:                                                           # loop across constrained outages
        volt_penalty += c[0]                                                                        # increment flow_penalty
    volt_penalty = round(volt_penalty, 2)                                                           # round total penalty to 2 places
    voltage_constrained_outages = [x[1] for x in voltage_constrained_outages]                       # return only the keys of the contrained outages

    if has_voltage_constraint:                                                                      # if constraint exists...
        num_text = '{0:<2d}'.format(len(voltage_constrained_outages))                               # assign number of constraint text
        penalty_text = 'PENALTY = ' + str(volt_penalty)                                             # assign penalty text
        print('PASS{0:2d} HAS VOLTAGE CONSTRAINTS .....................................'.format(step), num_text, penalty_text)  # print constraints info

    return has_voltage_constraint, voltage_constrained_outages, volt_penalty


def get_loading_contrained_outages(c_net, solvedoutages, onlinegens, gendict, linedict, xfmrdict, step):
    loading_constrained_outages = []                                                                         # initialize constrained outages list
    loading_penalty = 0.0
    has_loading_constraint = False                                                                          # initialize contraint flag

    otext = ''                                                                                      # initialize outage text
    for o_key in solvedoutages:                                                                     # loop across solved contingencies
        net = copy.deepcopy(c_net)                                                                  # get fresh copy of network
        if o_key in onlinegens:                                                                     # check if a generator...
            otext = 'GEN '                                                                          # assign text
            g_idx = gendict[o_key]                                                                  # get generator index
            net.gen.in_service[g_idx] = False                                                       # switch off outaged generator
        elif o_key in linedict:                                                                     # check if a line...
            otext = 'LINE'                                                                          # assign text
            line_idx = linedict[o_key]                                                              # get line index
            net.line.in_service[line_idx] = False                                                   # switch out outaged line
        elif o_key in xfmrdict:                                                                     # check if a xfmr...
            otext = 'XFMR'                                                                          # assign text
            xfmr_idx = xfmrdict[o_key]                                                              # get xfmr index
            net.trafo.in_service[xfmr_idx] = False                                                  # switch out outaged xfmr
        try:                                                                                        # try straight powerflow solution
            pp.runpp(net, enforce_q_lims=True)                                                      # run powerflow
            maxloading = get_maxloading(net)                                                        # get max branch loading
            if maxloading > 100.0:                                                                  # if max loading < 100%
                has_loading_constraint = True                                                       # set constraint flag
                loading_constrained_outages.append([maxloading - 100.0, o_key])                     # add outage to constrained outages list
        except:                                                                                     # if no solution...
            print(otext, '{0:9s} NOT SOLVED USING SCOPF BASECASE .....................'.format(o_key))

    loading_constrained_outages.sort(reverse=True)                                                  # sort constrained outages list
    for c in loading_constrained_outages:                                                           # loop across constrained outages
        loading_penalty += c[0]                                                                     # increment loading_penalty
    loading_penalty = round(loading_penalty, 1)                                                     # round total loading_penalty to 3 places
    loading_constrained_outages = [x[1] for x in loading_constrained_outages]                       # return only the keys of the contrained outages

    if has_loading_constraint:                                                                      # if constraint exists...
        num_text = '{0:<2d}'.format(len(loading_constrained_outages))                               # assign constraint text
        penalty_text = 'PENALTY = ' + str(loading_penalty)                                          # assign loading_penalty text
        print('PASS{0:2d} HAS LOADING CONSTRAINTS .....................................'.format(step), num_text, penalty_text)     # print constraints info

    return has_loading_constraint, loading_constrained_outages, loading_penalty


def print_dataframes_results(_net):
    pdoptions.display.max_columns = 100
    pdoptions.display.max_rows = 10000
    pdoptions.display.max_colwidth = 100
    pdoptions.display.width = None
    pdoptions.display.precision = 6
    # print()
    # print('BUS DATAFRAME')
    # print(_net.bus)
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
    # print('EXT GRID DATAFRAME')
    # print(_net.ext_grid)
    # print()
    # print('EXT GRID RESULTS')
    # print(_net.res_ext_grid)
    # print()
    return


# =================================================================================================
# -- MYPYTHON_1 -----------------------------------------------------------------------------------
# =================================================================================================
if __name__ == "__main__":
    print('PATH TO MYPYTHON1 =', cwd)
    print()
    start_time = time.time()

    # =============================================================================================
    # -- GET RAW,ROP,INL,CON DATA FROM FILES ------------------------------------------------------
    # =============================================================================================
    print('------------------------- READING RAW DATA -------------------------')
    areas = []
    gdisp_dict = {}
    pdisp_dict = {}
    pwl_dict0 = {}
    pwl_dict = {}
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

    # -- GET PWL TABLES FROM ACTIVE POWER DISPATCH TABLES -----------------------------------------
    for pdisp in raw_data.rop.active_power_dispatch_records.values():
        pdisp_dict.update({pdisp.tbl: pdisp.ctbl})

    # -- GET PWL DATA FROM PWL COST TABLES --------------------------------------------------------
    for pwldata in raw_data.rop.piecewise_linear_cost_functions.values():
        pwl_dict0.update({pwldata.ltbl: []})
        pwl_dict.update({pwldata.ltbl: []})
        for pair in pwldata.points:
            pwl_dict0[pwldata.ltbl].append([pair.x, pair.y])

    for tbl in pwl_dict0:
        for j in range(len(pwl_dict0[tbl]) - 1):
            x0 = pwl_dict0[tbl][j][0]
            y0 = pwl_dict0[tbl][j][1]
            x1 = pwl_dict0[tbl][j + 1][0]
            y1 = pwl_dict0[tbl][j + 1][1]
            slope = (y1 - y0) / (x1 - x0)
            pwl_dict[tbl].append([x0, x1, slope])

    for gkey in gdisp_dict:
        disptablekey = gdisp_dict[gkey]
        costtablekey = pdisp_dict[disptablekey]
        pcostdata0 = pwl_dict0[costtablekey]
        pcostdata = pwl_dict[costtablekey]
        pwlcost_dict0.update({gkey: pcostdata0})
        pwlcost_dict.update({gkey: pcostdata})

    # -- GET GENERATOR PARTICIPATION FACTORS ------------------------------------------------------
    for pf_record in raw_data.inl.generator_inl_records.values():
        gkey = str(pf_record.i) + '-' + pf_record.id
        pfactor_dict.update({gkey: pf_record.r})

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

    # -- GET SWING BUS FROM RAW BUSDATA -----------------------------------------------------------
    swingbus, swing_kv, swing_angle, swing_vlow, swing_vhigh = get_swingbus_data(raw_data.raw.buses)

    # -- GET SWING GEN DATA FROM GENDATA (REMOVE SWING GEN FROM GENDATA) --------------------------
    swgens_data = get_swgens_data(swingbus, raw_data.raw.generators)

    # -- GET ALTERNATE SWING BUS ------------------------------------------------------------------
    alt_swingbus, alt_sw_genkey = get_alt_swingbus(raw_data.raw.generators, raw_data.raw.buses, swingbus, swing_kv, swgens_data[0][6])

    # =============================================================================================
    # == CREATE NETWORK ===========================================================================
    # =============================================================================================
    print('------------------------ CREATING NETWORKS -------------------------')
    create_starttime = time.time()
    net_a = pp.create_empty_network('net_a', 60.0, mva_base)
    net_c = pp.create_empty_network('net_c', 60.0, mva_base)

    # == ADD BUSES TO NETWORK =====================================================================
    # buses = (bus.i, bus.name, bus.ide, bus.baskv, bus.area, bus.vm, bus.va, bus.nvhi, bus.nvlo, bus.evhi, bus.evlo)
    print('ADD BUSES ..........................................................')
    bus_dict = {}
    busnomkv_dict = {}
    buskv_dict = {}
    busarea_dict = {}
    busidxs = []
    areas = []
    for bus in raw_data.raw.buses.values():
        busnum = bus.i
        busnomkv = bus.baskv
        busarea = bus.area
        buskv = bus.vm
        if busnum == swingbus:
            sw_vmax_a = bus.nvhi
            sw_vmin_a = bus.nvlo
            sw_vmax_c = bus.evhi
            sw_vmin_c = bus.evlo
        # -- BASE NETWORK -------------------------------------------------------------------------
        pp.create_bus(net_a, vn_kv=busnomkv, name=bus.name, zone=busarea, max_vm_pu=bus.nvhi-1e-4, min_vm_pu=bus.nvlo+1e-4, in_service=True, index=busnum)
        # -- CONTINGENCY NETWORK ------------------------------------------------------------------
        idx = pp.create_bus(net_c, vn_kv=busnomkv, name=bus.name, zone=busarea, max_vm_pu=bus.evhi-1e-4, min_vm_pu=bus.evlo+1e-4, in_service=True, index=busnum)
        if busnum == swingbus:
            swingbus_idx = idx
        bus_dict.update({busnum: [busnomkv, bus.nvlo, bus.nvhi]})
        busnomkv_dict.update({busnum: busnomkv})
        buskv_dict.update({busnum: buskv})
        busarea_dict.update({busnum: busarea})
        busidxs.append(idx)
        areas.append(busarea)
    areas = list(set(areas))
    areas.sort()

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
        loadp = load.pl
        loadq = load.ql
        loadmva = math.sqrt(loadp ** 2 + loadq ** 2)
        pp.create_load(net_a, bus=loadbus, p_mw=loadp, q_mvar=loadq, sn_mva=loadmva, name=loadname)
        pp.create_load(net_c, bus=loadbus, p_mw=loadp, q_mvar=loadq, sn_mva=loadmva, name=loadname)

    # == ADD GENERATORS TO NETWORK ================================================================
    print('ADD GENERATORS .....................................................')
    genbuses = []
    Gids = []
    gen_dict = {}
    genidx_dict = {}
    swinggen_idxs = []
    genarea_dict = {}
    genidxs = []
    genbus_dict = {}
    participating_gens = []
    area_participating_gens = {}
    for area in areas:
        area_participating_gens.update({area: []})
        
    # -- ADD SWING GENERATORS ---------------------------------------------------------------------
    #  swgens_data = (key, id, pgen, qgen, qmax, qmin, vreg, pmax, pmin, status)
    for swgen_data in swgens_data:
        swing_kv = busnomkv_dict[swingbus]
        genbus = swingbus
        genkey = swgen_data[0]
        gid = swgen_data[1]
        pgen = swgen_data[2]
        qgen = swgen_data[3]
        qmax = swgen_data[4]
        qmin = swgen_data[5]
        pmax = swgen_data[7]
        pmin = swgen_data[8]
        vreg = swgen_data[6]
        if SWVREG == 1:
            vreg = SwVreg_Custom
        status = swgen_data[9]
        nomkv = busnomkv_dict[genbus]
        genmva = math.sqrt(pmax ** 2 + qmax ** 2)
        power_factor = pmax / genmva
        if genkey in pwlcost_dict:
            pcostdata = pwlcost_dict[genkey]
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin,  max_q_mvar=qmax, min_q_mvar=qmin,
                                rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv, type='SWGEN', controllable=True, in_service=status)
            pp.create_pwl_cost(net_a, idx, 'gen', pcostdata)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin,  max_q_mvar=qmax, min_q_mvar=qmin,
                          rdss_pu=0.005, xdss_pu=0.25,  cos_phi=power_factor, vn_kv=nomkv, type='SWGEN', controllable=True, in_service=status, index=idx, slack=False)
            pp.create_pwl_cost(net_c, idx, 'gen', pcostdata)
            if status:
                participating_gens.append(genkey)
                area_participating_gens[busarea_dict[genbus]].append(genkey)
        else:
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin,  max_q_mvar=qmax, min_q_mvar=qmin,
                                rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv, type='SWGEN', controllable=False, in_service=status)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin,  max_q_mvar=qmax, min_q_mvar=qmin,
                          rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv,  type='SWGEN', controllable=False, in_service=status, index=idx)
        swing_vreg = vreg
        swinggen_idxs.append(idx)
        gen_dict.update({genkey: idx})
        genbuses.append(genbus)
        Gids.append("'" + gid + "'")
        genarea_dict.update({genkey: busarea_dict[genbus]})
        genidxs.append(idx)
        genidx_dict.update({genbus: idx})
        genbus_dict.update({idx: genbus})

    # -- ADD REMAINING GENERATOR ------------------------------------------------------------------
    # gens = (gen.i, gen.id, gen.pg, gen.qg, gen.qt, gen.qb, gen.vs, gen.pt, gen.pb, gen.stat)
    for gen in raw_data.raw.generators.values():
        genbus = gen.i
        gid = gen.id
        pgen = gen.pg
        qgen = gen.qg
        qmax = gen.qt
        qmin = gen.qb
        pmax = gen.pt
        pmin = gen.pb
        vreg = gen.vs
        nomkv = busnomkv_dict[genbus]
        genmva = math.sqrt(pmax ** 2 + qmax ** 2)
        power_factor = pmax / genmva
        if GVREG:
            vreg = Gvreg_Custom
        status = bool(gen.stat)
        pcostdata = None
        genkey = str(genbus) + '-' + str(gid)
        if genkey in pwlcost_dict:
            pcostdata = pwlcost_dict[genkey]
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=qmax, min_q_mvar=qmin,
                                rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv, type='GEN', controllable=True, in_service=status)
            pp.create_pwl_cost(net_a, idx, 'gen', pcostdata)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=qmax, min_q_mvar=qmin,
                          rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv,  type='GEN', controllable=True, in_service=status, index=idx)
            pp.create_pwl_cost(net_c, idx, 'gen', pcostdata)
            if status:
                participating_gens.append(genkey)
                area_participating_gens[busarea_dict[genbus]].append(genkey)

        else:
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, name=genkey, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=qmax, min_q_mvar=qmin,
                                rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv,  type='GEN', controllable=False, in_service=status)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, name=genkey, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=qmax, min_q_mvar=qmin,
                          rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv,  type='GEN', controllable=False, in_service=status, index=idx)
        Gids.append("'" + gid + "'")
        genidx_dict.update({genbus: idx})
        genbuses.append(genbus)
        genarea_dict.update({genkey: busarea_dict[genbus]})
        genidxs.append(idx)
        gen_dict.update({genkey: idx})
        genbus_dict.update({idx: genbus})

    # == ADD FIXED SHUNT DATA TO NETWORK ==========================================================
    # fixshunt = (fxshunt.i, fxshunt.id, fxshunt.status, fxshunt.gl, fxshunt.bl)
    fxshidx_dict = {}
    if raw_data.raw.fixed_shunts.values():
        print('ADD FIXED SHUNTS ...................................................')
        for fxshunt in raw_data.raw.fixed_shunts.values():
            status = bool(fxshunt.status)
            if not status:
                continue
            shuntbus = fxshunt.i
            shuntname = str(shuntbus) + '-FX'
            # mw = -fxshunt.gl
            # mvar = -fxshunt.bl
            nomkv = busnomkv_dict[shuntbus]

            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_shunt(net_a, shuntbus, vn_kv=nomkv, q_mvar=-fxshunt.bl, p_mw=fxshunt.gl, step=1, max_step=True, name=shuntname)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_shunt(net_c, shuntbus, vn_kv=nomkv, q_mvar=-fxshunt.bl, p_mw=fxshunt.gl, step=1, max_step=True, name=shuntname, index=idx)
            fxshidx_dict.update({shuntbus: idx})

    # == ADD SWITCHED SHUNTS TO NETWORK ===========================================================
    # -- (SWSHUNTS ARE MODELED AS Q-GENERATORS) ---------------------------------------------------
    # swshunt = (swshunt.i, swshunt.binit, swshunt.n1, swshunt.b1, swshunt.n2, swshunt.b2, swshunt.n3, swshunt.b3, swshunt.n4, swshunt.b4,
    #            swshunt.n5, swshunt.b5, swshunt.n6, swshunt.b6, swshunt.n7, swshunt.b7, swshunt.n8, swshunt.b8, swshunt.stat)
    # gens = (gen.i, gen.id, gen.pg, gen.qg, gen.qt, gen.qb, gen.pt, gen.pb, gen.stat)
    swshidx_dict = {}
    swshidxs = []
    swsh_dict = {}
    swshbus_dict = {}
    area_swhunts = {}
    for area in areas:
        area_swhunts.update({area: []})

    if raw_data.raw.switched_shunts.values():
        print('ADD SWITCHED SHUNTS ................................................')
        for swshunt in raw_data.raw.switched_shunts.values():
            status = bool(swshunt.stat)
            if not status:
                continue
            shuntbus = swshunt.i
            swshkey = str(shuntbus) + '-SW'
            vreg = buskv_dict[shuntbus]
            nomkv = busnomkv_dict[shuntbus]
            if SWSHVREG:
                vreg = SwShVreg_Custom
            if shuntbus in genbuses:
                gidx = genidx_dict[shuntbus]
                if net_a.gen.loc[gidx, 'in_service']:
                    vreg = net_a.gen.loc[gidx, 'vm_pu']
            steps = [swshunt.n1, swshunt.n2, swshunt.n3, swshunt.n4, swshunt.n5, swshunt.n6, swshunt.n7, swshunt.n8]
            mvars = [swshunt.b1, swshunt.b2, swshunt.b3, swshunt.b4, swshunt.b5, swshunt.b6, swshunt.b7, swshunt.b8]
            total_qmin = 0.0
            total_qmax = 0.0
            for j in range(len(mvars)):
                if mvars[j] < 0.0:
                    total_qmin += steps[j] * mvars[j]
                elif mvars[j] > 0.0:
                    total_qmax += steps[j] * mvars[j]
            pgen = 0.0
            pmax = 0.0
            pmin = 0.0
            shuntmva = math.sqrt(pmax ** 2 + total_qmax ** 2)

            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, shuntbus, pgen, vm_pu=vreg, sn_mva=shuntmva, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=total_qmax, min_q_mvar=total_qmin,
                                rdss_pu=99.99, xdss_pu=99.99, cos_phi=1e-6, vn_kv=nomkv, controllable=True, name=swshkey, type='SWSH')
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_gen(net_c, shuntbus, pgen, vm_pu=vreg, sn_mva=shuntmva, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=total_qmax, min_q_mvar=total_qmin,
                          rdss_pu=99.9, xdss_pu=99.99, cos_phi=1e-6, vn_kv=nomkv, controllable=True, name=swshkey, type='SWSH', index=idx)
            swshidx_dict.update({shuntbus: idx})
            swshidxs.append(idx)
            swsh_dict.update({swshkey: idx})
            swshbus_dict.update({idx: shuntbus})
            area_swhunts[busarea_dict[shuntbus]].append(swshkey)

    # == ADD LINES TO NETWORK =====================================================================
    # line = (line.i, line.j, line.ckt, line.r, line.x, line.b, line.ratea, line.ratec, line.st, line.len, line.met)
    line_dict = {}
    line_ratea_dict = {}
    lineidxs = []
    branch_areas = {}
    print('ADD LINES ..........................................................')
    for line in raw_data.raw.nontransformer_branches.values():
        frombus = line.i
        tobus = line.j
        ckt = line.ckt
        linekey = str(frombus) + '-' + str(tobus) + '-' + ckt

        # if line.met:
        #     frombus, tobus = tobus, frombus

        status = bool(line.st)
        length = line.len
        if length == 0.0:
            length = 1.0
        kv = busnomkv_dict[frombus]
        zbase = kv ** 2 / mva_base
        r_pu = line.r / length
        x_pu = line.x / length
        b_pu = line.b / length
        r = r_pu * zbase
        x = x_pu * zbase
        b = b_pu / zbase
        capacitance = 1e9 * b / (2 * math.pi * 60.0)
        base_mva_rating = line.ratea
        mva_rating = line.ratec
        i_rating_a = base_mva_rating / (math.sqrt(3) * kv)
        i_rating_c = mva_rating / (math.sqrt(3) * kv)
        # -- BASE NETWORK -------------------------------------------------------------------------
        idx = pp.create_line_from_parameters(net_a, frombus, tobus, length, r, x, capacitance, i_rating_a, name=linekey, max_loading_percent=MaxLoading, in_service=status)
        # -- CONTINGENCY NETWORK ------------------------------------------------------------------
        pp.create_line_from_parameters(net_c, frombus, tobus, length, r, x, capacitance, i_rating_c, name=linekey, max_loading_percent=MaxLoading, in_service=status, index=idx)
        line_dict.update({linekey: idx})
        lineidxs.append(idx)
        branch_areas.update({linekey: [busarea_dict[frombus], busarea_dict[tobus]]})

    # == ADD 2W TRANSFORMERS TO NETWORK ===========================================================
    # 2wxfmr = (xfmr.i, xfmr.j, xfmr.ckt, xfmr.mag1, xfmr.mag2, xfmr.r12, xfmr.x12, xfmr.windv1, xfmr.nomv1,
    #           xfmr.ang1, xfmr.rata1, xfmr.ratc1, xfmr.windv2, xfmr.nomv2, xfmr.stat)
    xfmr_dict = {}
    xfmr_ratea_dict = {}
    xfmridxs = []
    print('ADD 2W TRANSFORMERS ................................................')
    for xfmr in raw_data.raw.transformers.values():
        status = bool(xfmr.stat)
        frombus = xfmr.i
        tobus = xfmr.j
        ckt = xfmr.ckt
        xfmrkey = str(frombus) + '-' + str(tobus) + '-' + ckt                   # DEFINE XFMR KEY

        wind1 = xfmr.i                                                          # GET BUS CONNECTED TO WINDING1
        wind2 = xfmr.j                                                          # GET BUS CONNECTED TO WINDING2
        lowbus = wind1                                                          # ASSUME LOWBUS CONNECTED TO WINDING1
        lowkv = busnomkv_dict[wind1]                                            # GET KV OF ASSUMED LOWBUS
        lv_tap = xfmr.windv1                                                    # GET ASSUMED LOWVOLTAGE NLTC
        highbus = wind2                                                         # ASSUME HIGHBUS CONNECTED WINDING2
        highkv = busnomkv_dict[wind2]                                           # GET KV OF ASSUMED HIGHBUS
        hv_tap = xfmr.windv2                                                    # GET ASSUMED HIGHVOLTAGE NLTC
        tapside = 'lv'

        if lowkv > highkv:                                                      # IF WINDING1 IS CONNECTED TO HIGHBUS...
            highbus, lowbus = lowbus, highbus                                   # SWAP HIGHBUS, LOWBUS
            highkv, lowkv = lowkv, highkv                                       # SWAP HIGHKV, LOWKV
            hv_tap, lv_tap = lv_tap, hv_tap                                     # SWAP HIGHVOLTAGE NLTC, LOWVOLTAGE NLTC
            tapside = 'hv'
        net_tap = lv_tap / hv_tap                                               # NET TAP SETTING ON LOWSIDE

        r_pu_sbase = xfmr.r12                                                   # RPU @ MVA_BASE (FROM RAW DATA)
        x_pu_sbase = xfmr.x12                                                   # XPU @ MVA_BASE (FROM RAW DATA)

        # -- RATE A 'NAMEPLATE' IMPEDANCE -------------------------------------
        r_pu_a = r_pu_sbase * xfmr.rata1 / mva_base                             # PANDAPOWER USES RATING AS TEST MVA
        x_pu_a = x_pu_sbase * xfmr.rata1 / mva_base                             # SO CONVERT TO RATE A BASE
        z_pu_a = math.sqrt(r_pu_a ** 2 + x_pu_a ** 2)                           # CALCULATE RATE A 'NAMEPLATE' PU IMPEDANCE
        z_pct_a = 100.0 * z_pu_a                                                # PERCENT IMPEDANCE (FOR PANDAPOWER XFMR)
        r_pct_a = 100.0 * r_pu_a                                                # PERCENT RESISTANCE  (FOR PANDAPOWER XFMR)

        # -- RATE C 'NAMEPLATE' IMPEDANCE -------------------------------------
        r_pu_c = r_pu_sbase * xfmr.ratc1 / mva_base                             # PANDAPOWER USES RATING AS TEST MVA
        x_pu_c = x_pu_sbase * xfmr.ratc1 / mva_base                             # SO CONVERT TO RATE C BASE
        z_pu_c = math.sqrt(r_pu_c ** 2 + x_pu_c ** 2)                           # CALCULATE RATE C 'NAMEPLATE' PU IMPEDANCE
        z_pct_c = 100.0 * z_pu_c                                                # PERCENT IMPEDANCE (FOR PANDAPOWER XFMR)
        r_pct_c = 100.0 * r_pu_c                                                # PERCENT RESISTANCE  (FOR PANDAPOWER XFMR)

        shuntname = str(highbus) + '-FXMAG'
        fx_p = 0.0
        fx_q = 0.0
        if xfmr.mag1 != 0.0 or xfmr.mag2 != 0.0:
            fx_p = mva_base * xfmr.mag1
            fx_q = -mva_base * xfmr.mag2

        # -- BASE NETWORK MAGNETIZING ADMITTANCE --------------------------------------------------
        idx = pp.create_shunt(net_a, wind1, q_mvar=fx_q, p_mw=fx_p, step=1, max_step=True, name=shuntname)
        # -- CONTINGENCY NETWORK MAGNETIZING ADMITTANCE --------------------------------------------------
        pp.create_shunt(net_c, wind1, q_mvar=fx_q, p_mw=fx_p, step=1, max_step=True, name=shuntname, index=idx)
        fxshidx_dict.update({wind1: idx})

        # -- TAP SETTINGS -----------------------------------------------------
        tapstepdegree = 0.0
        tapphaseshifter = False
        shiftdegree = xfmr.ang1
        tapside = 'lv'
        tapmax = 2
        tapneutral = 0
        tapmin = -2
        tapsteppct = 100.0 * abs(1 - net_tap)
        if net_tap > 1.0:
            tappos = 1
        elif net_tap == 1.0:
            tappos = 0
        elif net_tap < 1.0:
            tappos = -1

        pkw_a = 0.0
        pkw_c = 0.0
        i0_a = 0.0
        i0_c = 0.0
        # Gm = xfmr.mag1                                                          # MAGNETIZING CONDUCTANCE AT SBASE
        # Bm = xfmr.mag2                                                          # MAGNETIZING SUSCEPTANCE AT SBASE
        # if abs(Gm) > 0.0:
        #     pkw_a = 1000.0 * xfmr.rata1 ** 2 / Gm * mva_base
        #     pkw_a = 1000.0 * xfmr.ratc1 ** 2 / Gm * mva_base
        # i0_a = 100.0 * math.sqrt(Gm ** 2 + Bm ** 2) * mva_base / xfmr.rata1
        # i0_c = 100.0 * math.sqrt(Gm ** 2 + Bm ** 2) * mva_base / xfmr.ratc1

        # -- BASE NETWORK -------------------------------------------------------------------------
        idx = pp.create_transformer_from_parameters(net_a, highbus, lowbus, xfmr.rata1, highkv, lowkv, r_pct_a, z_pct_a, pfe_kw=pkw_a, i0_percent=i0_a,
                                                    shift_degree=shiftdegree, tap_side=tapside, tap_neutral=tapneutral, tap_max=tapmax, tap_min=tapmin,
                                                    tap_step_percent=tapsteppct, tap_step_degree=tapstepdegree, tap_pos=tappos, tap_phase_shifter=False,
                                                    in_service=status, name=xfmrkey, max_loading_percent=MaxLoading, parallel=1, df=1.0)

        # -- CONTINGENCY NETWORK ------------------------------------------------------------------
        pp.create_transformer_from_parameters(net_c, highbus, lowbus, xfmr.ratc1, highkv, lowkv, r_pct_c, z_pct_c, pfe_kw=pkw_c, i0_percent=i0_c,
                                              shift_degree=shiftdegree, tap_side=tapside, tap_neutral=tapneutral, tap_max=tapmax, tap_min=tapmin,
                                              tap_step_percent=tapsteppct, tap_step_degree=tapstepdegree, tap_pos=tappos, tap_phase_shifter=False,
                                              in_service=status, name=xfmrkey, max_loading_percent=MaxLoading, parallel=1, df=1.0, index=idx)
        xfmr_dict.update({xfmrkey: idx})
        xfmr_ratea_dict.update({xfmrkey: xfmr.rata1})
        xfmridxs.append(idx)
        branch_areas.update({xfmrkey: [busarea_dict[highbus], busarea_dict[lowbus]]})

    for bkey in branch_areas:
        branch_areas[bkey] = list(set(branch_areas[bkey]))

    # == ADD EXTERNAL GRID ========================================================================
    # == WITH DUMMY TIE TO RAW SWING BUS ==========================================================
    ext_tie_rating = 1e5/(math.sqrt(3) * swing_kv)                                                 # CURRENT RATING USING SWING KV

    # -- CREATE BASE NETWORK EXTERNAL GRID --------------------------------------------------------
    ext_grid_idx = pp.create_bus(net_a, vn_kv=swing_kv, name='Ex_Grid_Bus', max_vm_pu=sw_vmax_a, min_vm_pu=sw_vmin_a)
    tie_idx = pp.create_line_from_parameters(net_a, swingbus, ext_grid_idx, 1.0, 0.0, 0.001, 0.0, ext_tie_rating, name='Swing-Tie', max_loading_percent=100.0)
    pp.create_ext_grid(net_a, ext_grid_idx, vm_pu=swing_vreg, va_degree=swing_angle, max_p_mw=1e-3, min_p_mw=-1e-3, max_q_mvar=1e-3, min_q_mvar=-1e-3,
                       s_sc_max_mva=1.0, s_sc_min_mva=1.0, rx_max=0.011, rx_min=0.01, index=ext_grid_idx)
    pp.create_poly_cost(net_a, ext_grid_idx, 'ext_grid', cp1_eur_per_mw=0, cp0_eur=1e9, type='p')
    # pp.create_poly_cost(net_a, ext_grid_idx, 'ext_grid', cq1_eur_per_mvar=1, cq0_eur=1e6, type='q')

    # -- CREATE CONTINGENCY NETWORK EXTERNAL GRID -------------------------------------------------
    pp.create_bus(net_c, vn_kv=swing_kv, name='Ex_Grid_Bus', max_vm_pu=sw_vmax_c, min_vm_pu=sw_vmin_c, index=ext_grid_idx)
    tie_idx = pp.create_line_from_parameters(net_c, swingbus, ext_grid_idx, 1.0, 0.0, 0.001, 0.0, ext_tie_rating, name='Swing-Tie', in_service=True, max_loading_percent=100.0)
    alt_tie_idx = pp.create_line_from_parameters(net_c, alt_swingbus, ext_grid_idx, 1.0, 0.0, 0.001, 0.0, ext_tie_rating, name='Swing-Tie', in_service=False, max_loading_percent=100.0)
    pp.create_ext_grid(net_c, ext_grid_idx, vm_pu=swing_vreg, va_degree=swing_angle, max_p_mw=1e-3, min_p_mw=-1e-3, max_q_mvar=1e-3, min_q_mvar=-1e-3,
                       s_sc_max_mva=1.0, s_sc_min_mva=1.0, rx_max=0.01, rx_min=0.01, index=ext_grid_idx)
    pp.create_poly_cost(net_c, ext_grid_idx, 'ext_grid', cp1_eur_per_mw=0, cp0_eur=1e9, cq1_eur_per_mvar=0, cq0_eur=1e9)
    # pp.create_poly_cost(net_c, ext_grid_idx, 'ext_grid', cq1_eur_per_mvar=0, cq0_eur=1e9, type='q')

    print('   NETWORKS CREATED ................................................', round(time.time() - create_starttime, 3), 'SECONDS')

    # == MISC NETWORK DATA ========================================================================
    goutagekeys = list(outage_dict['gen'].keys())                                                   # GET OUTAGED GENERATOR KEYS
    boutagekeys = list(outage_dict['branch'].keys())                                                # GET OUTAGED BRANCH KEYS

    online_gens = []                                                                                # INITIALIZE ONLINE GENERATOR LIST
    for gkey in gen_dict:                                                                           # LOOP ACROSS GENERATOR KEYS
        gidx = gen_dict[gkey]                                                                       # GET GENERATOR INDEX
        if not net_a.gen.loc[gidx, 'in_service']:                                                   # CHECK IF GENERATOR IS ONLINE
            continue                                                                                # IF NOT ONLINE, GET NEXT GENERATOR KEY
        online_gens.append(gkey)                                                                    # ADD ONLINE GENERATOR TO LIST

    levels_out = 3                                                                                  # DEFINE BUS NLEVELS FOR ADJUSTING SWING GEN SETPOINT
    nlevel_buses = [swingbus]                                                                       # INITIALIZE BUS LIST
    for i in range(levels_out):                                                                     # LOOP THROUGH HOW MANY LEVELS...
        nlevel_buses = pp.get_connected_buses(net_a, nlevel_buses, respect_in_service=True)         # NEW BUSES ARE ADDED TO BUSLIST
    nlevel_buses = [x for x in nlevel_buses if x != ext_grid_idx]                                   # REMOVE EXTGRID BUS FROM BUSLIST
    # =============================================================================================

    # -- SOLVE INITIAL NETWORKS WITH STRAIGHT POWERFLOW -------------------------------------------
    solve_starttime = time.time()
    pp.runpp(net_a, enforce_q_lims=True)                                                            # SOLVE INITIAL BASE NETWORK
    pp.runpp(net_c, enforce_q_lims=True)                                                            # SOLVE INITIAL CONTINGENCY NETWORK
    print('   NETWORKS SOLVED .................................................', round(time.time() - solve_starttime, 3), 'SECONDS')

    net = copy.deepcopy(net_a)
    pp.runopp(net, init='pf', enforce_q_lims=True)                                                  # RUN OPF ON THIS NETWORK
    net_a.gen['p_mw'] = net.res_gen['p_mw']                                                         # SET THIS NETWORK GENERATORS POWER TO OPF RESULTS
    net_c.gen['p_mw'] = net.res_gen['p_mw']                                                         # SET THIS NETWORK GENERATORS POWER TO OPF RESULTS

    for gkey in gen_dict:                                                                           # LOOP ACROSS GENERATOR KEYS
        gidx = gen_dict[gkey]                                                                       # GET GENERATOR INDEX
        genbus = genbus_dict[gidx]                                                                  # GET GENERATOR BUS
        if genbus == swingbus:                                                                      # CHECK IF SWING BUS...
            continue                                                                                # IF SWING BUS, GET NEXT GENERATOR
        net_a.gen.loc[gidx, 'vm_pu'] = net.res_bus.loc[genbus, 'vm_pu']                             # SET THIS NETWORK GENS VREG TO OPF RESULTS
        net_c.gen.loc[gidx, 'vm_pu'] = net.res_bus.loc[genbus, 'vm_pu']                             # SET THIS NETWORK GENS VREG TO OPF RESULTS

    for shkey in swsh_dict:                                                                         # LOOP ACROSS SWSHUNT KEYS
        shidx = swsh_dict[shkey]                                                                    # GET SWSHUNT INDEX
        shbus = swshbus_dict[shidx]                                                                 # GET SWSHUNT BUS
        net_a.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']                             # SET THIS NETWORK SWSHUNT VREG TO OPF RESULTS
        net_c.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']                             # SET THIS NETWORK SWSHUNT VREG TO OPF RESULTS

    # -- ATTEMPT AT BETTER ESTIMATE FOR SWING VREG ------------------------------------------------
    nlevel_buses_v = [x for x in net.res_bus.loc[nlevel_buses, 'vm_pu']]                            # GET LIST OF NLEVEL BUS VOLTAGES
    max_v = max(nlevel_buses_v)                                                                     # GET MAX VOLTAGE OF NLEVEL BUSES
    net_a.gen.loc[swinggen_idxs, 'vm_pu'] = max_v                                                   # SET SWING GENS VREG = MAX OF NLEVEL BUSES VOLTAGE
    net_a.ext_grid.loc[ext_grid_idx, 'vm_pu'] = max_v                                               # SET EXTGRID VREG = MAX OF NLEVEL BUSES VOLTAGE
    net_c.gen.loc[swinggen_idxs, 'vm_pu'] = max_v                                                   # SET SWING GENS VREG = MAX OF NLEVEL BUSES VOLTAGE
    net_c.ext_grid.loc[ext_grid_idx, 'vm_pu'] = max_v                                               # SET EXTGRID VREG = MAX OF NLEVEL BUSES VOLTAGE
    pp.runpp(net_a, enforce_q_lims=True)                                                            # SOLVE INITIAL BASE NETWORK
    pp.runpp(net_c, enforce_q_lims=True)                                                            # SOLVE INITIAL CONTINGENCY NETWORK

    # *********************************************************************************************
    # -- FIND BASECASE SCOPF OPERATING POINT ------------------------------------------------------
    # *********************************************************************************************
    print('-------------------- ATTEMPTING BASECASE SCOPF ---------------------')
    opf_startime = time.time()

    # ---------------------------------------------------------------------------------------------
    # -- TRY TO MINIMIZE LOADING CONSTRAINTS ------------------------------------------------------
    # ---------------------------------------------------------------------------------------------
    loading_penalties = []
    previous_loading_penalties = []
    loading_step = 0

    # -- CHECK CONTINGENCIES FOR NO SOLUTION ------------------------------------------------------
    nosolve, solved_outages, nosolve_outages = get_nosolves(net_c, goutagekeys, boutagekeys, gen_dict, line_dict, xfmr_dict, loading_step)

    # -- FIND OUTAGES RESULTING IN LOADING CONSTRAINTS --------------------------------------------
    constraint, constrained_outages, penalty = get_loading_contrained_outages(net_c, solved_outages, online_gens, gen_dict, line_dict, xfmr_dict, loading_step)
    loading_penalties.append(penalty)                                                               # ADD LOADING PENALTY TO LIST
    loading_penalties = list(set(loading_penalties))                                                # REMOVE DUPLICATES FROM LIST
    loading_penalties.sort()                                                                        # SORT LOADING LIST

    net = copy.deepcopy(net_c)                                                                      # INITIALIZE SCOPF NETWORK

    # -- IF LOADING CONSTRAINT, LOOP TO FIND MINIMUM PENALTY --------------------------------------
    loading_step += 1                                                                               # INCREMENT ITERATOR
    stop = False                                                                                    # INITIALIZE WHILE LOOP STOP FLAG
    while constraint and not stop and loading_step < 10:                                            # LOOP WHILE THERE IS A CONSTRAINT
        constraint = False                                                                          # SET CONSTRAINT FLAG
        previous_net = copy.deepcopy(net)                                                           # GET COPY OF NETWORK BEFORE OUTAGE
        o_key = constrained_outages[0]                                                              # GET WORST CONSTRAINED OUTAGE
        if o_key in online_gens:                                                                    # CHECK IF A GENERATOR...
            o_type = 'GEN'                                                                          # ASSIGN TEXT
            o_idx = gen_dict[o_key]                                                                 # GET GENERATOR INDEX
            net.gen.in_service[o_idx] = False                                                       # SWITCH OFF OUTAGED GENERATOR
        elif o_key in line_dict:                                                                    # CHECK IF A LINE...
            o_type = 'LINE'                                                                         # ASSIGN TEXT
            o_idx = line_dict[o_key]                                                                # GET LINE INDEX
            net.line.in_service[o_idx] = False                                                      # SWITCH OUT OUTAGED LINE
        elif o_key in xfmr_dict:                                                                    # CHECK IF A XFMR...
            o_type = 'XFMR'                                                                         # ASSIGN TEXT
            o_idx = xfmr_dict[o_key]                                                                # GET XFMR INDEX
            net.trafo.in_service[o_idx] = False                                                     # SWITCH OUT OUTAGED XFMR

        try:                                                                                        # TRY TO SOLVE WITH OPF
            pp.runopp(net, init='pf', enforce_q_lims=True)                                          # RUN OPF ON THIS NETWORK
            net.gen['p_mw'] = net.res_gen['p_mw']                                                   # SET THIS NETWORK GENERATORS POWER TO OPF RESULTS
            for gkey in gen_dict:                                                                   # LOOP ACROSS GENERATOR KEYS
                gidx = gen_dict[gkey]                                                               # GET GENERATOR INDEX
                genbus = genbus_dict[gidx]                                                          # GET GENERATOR BUS
                if genbus == swingbus:                                                              # CHECK IF SWING BUS...
                    continue                                                                        # IF SWING BUS, GET NEXT GENERATOR
                net.gen.loc[gidx, 'vm_pu'] = net.res_bus.loc[genbus, 'vm_pu']                       # SET THIS NETWORK GENS VREG TO OPF RESULTS
            for shkey in swsh_dict:                                                                 # LOOP ACROSS SWSHUNT KEYS
                shidx = swsh_dict[shkey]                                                            # GET SWSHUNT INDEX
                shbus = swshbus_dict[shidx]                                                         # GET SWSHUNT BUS
                net.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']                       # SET THIS NETWORK SWSHUNT VREG TO OPF RESULTS
            pp.runpp(net, enforce_q_lims=True)                                                      # RUN POWERFLOW

            if o_type == 'GEN':                                                                     # IF OUTAGE WAS A GENERATOR...
                net.gen.in_service[o_idx] = True                                                    # PUT GENERATOR BACK IN-SERVICE
            if o_type == 'LINE':                                                                    # IF OUTAGE WAS A LINE...
                net.line.in_service[o_idx] = True                                                   # PUT LINE BACK IN-SERVICE
            if o_type == 'XFMR':                                                                    # IF OUTAGE WAS A XFMR...
                net.trafo.in_service[o_idx] = True                                                  # PUT XFMR BACK IN-SERVICE
            pp.runpp(net, init='results', enforce_q_lims=True)                                      # RUN POWERFLOW

            previous_loading_penalties = list(loading_penalties)                                          # COPY OUTAGE penalties BEFORE RUNNING OUTAGES AGAIN
            nosolve, solved_outages, nosolve_outages = get_nosolves(net, goutagekeys, boutagekeys, gen_dict, line_dict, xfmr_dict, loading_step)
            constraint, constrained_outages, penalty = get_loading_contrained_outages(net, solved_outages, online_gens, gen_dict, line_dict, xfmr_dict, loading_step)

            loading_penalties.append(penalty)                                                     # ADD LOADING penalty TO penalties LIST
            loading_penalties = list(set(loading_penalties))                                              # REMOVE DUPLICATES FROM penalties LIST
            loading_penalties.sort()                                                                   # SORT LOADING penalties LIST
            if loading_penalties == previous_loading_penalties and penalty == min(loading_penalties):   # IF LIST REPEATS AND penalty IS MINIMUM...
                stop = True                                                                         # STOP LOOPING
        except:                                                                                             # IF NO OPF SOLUTION...
            print(o_type, '{0:9s} DID NOT SOLVE WITH OPF ..............................'.format(o_key))     # PRINT OUTAGE INFO
            net = previous_net                                                                              # LOAD PRE-OUTAGE NETWORK
        loading_step += 1

    # ---------------------------------------------------------------------------------------------
    # -- TRY TO MINIMIZE VOLTAGE CONSTRAINTS ------------------------------------------------------
    # ---------------------------------------------------------------------------------------------
    # voltage_penalties = []
    # previous_voltage_penalties = []
    # voltage_step = 0
    #
    # # -- CHECK CONTINGENCIES FOR NO SOLUTION ------------------------------------------------------
    # nosolve, solved_outages, nosolve_outages = get_nosolves(net, goutagekeys, boutagekeys, gen_dict, line_dict, xfmr_dict, voltage_step)
    #
    # # -- FIND OUTAGES RESULTING IN VOLTAGE CONSTRAINTS --------------------------------------------
    # constraint, constrained_outages, penalty = get_voltage_contrained_outages(net, solved_outages, online_gens, gen_dict, line_dict, xfmr_dict, bus_dict, voltage_step)
    # voltage_penalties.append(penalty)                                                              # ADD LOADING PENALTY TO LIST
    # voltage_penalties = list(set(voltage_penalties))                                               # REMOVE DUPLICATES FROM LIST
    # voltage_penalties.sort()                                                                       # SORT LIST
    #
    # # -- IF VOLTAGE CONSTRAINT, LOOP TO FIND MINIMUM PENALTY --------------------------------------
    # voltage_step += 1                                                                               # INCREMENT ITERATOR
    # stop = False                                                                                    # INITIALIZE WHILE LOOP STOP FLAG
    # # net = copy.deepcopy(net_c)                                                                      # INITIALIZE NETWORK
    # while constraint and not stop and voltage_step < 10:                                            # LOOP WHILE THERE IS A CONSTRAINT
    #     constraint = False                                                                          # SET constraint FLAG
    #     previous_net = copy.deepcopy(net)                                                           # GET COPY OF NETWORK BEFORE OUTAGE
    #     o_key = constrained_outages[0]                                                              # GET WORST CONSTRAINED OUTAGE
    #     if o_key in online_gens:                                                                    # CHECK IF A GENERATOR...
    #         o_type = 'GEN'                                                                          # ASSIGN TEXT
    #         o_idx = gen_dict[o_key]                                                                 # GET GENERATOR INDEX
    #         net.gen.in_service[o_idx] = False                                                       # SWITCH OFF OUTAGED GENERATOR
    #     elif o_key in line_dict:                                                                    # CHECK IF A LINE...
    #         o_type = 'LINE'                                                                         # ASSIGN TEXT
    #         o_idx = line_dict[o_key]                                                                # GET LINE INDEX
    #         net.line.in_service[o_idx] = False                                                      # SWITCH OUT OUTAGED LINE
    #     elif o_key in xfmr_dict:                                                                    # CHECK IF A XFMR...
    #         o_type = 'XFMR'                                                                         # ASSIGN TEXT
    #         o_idx = xfmr_dict[o_key]                                                                # GET XFMR INDEX
    #         net.trafo.in_service[o_idx] = False                                                     # SWITCH OUT OUTAGED XFMR
    #
    #     try:                                                                                        # TRY TO SOLVE WITH OPF
    #         pp.runopp(net, init='pf', enforce_q_lims=True)                                          # RUN OPF ON THIS NETWORK
    #         net.gen['p_mw'] = net.res_gen['p_mw']                                                   # SET THIS NETWORK GENERATORS POWER TO OPF RESULTS
    #         for gkey in gen_dict:                                                                   # LOOP ACROSS GENERATOR KEYS
    #             gidx = gen_dict[gkey]                                                               # GET GENERATOR INDEX
    #             genbus = genbus_dict[gidx]                                                          # GET GENERATOR BUS
    #             if genbus == swingbus:                                                              # CHECK IF SWING BUS...
    #                 continue                                                                        # IF SWING BUS, GET NEXT GENERATOR
    #             net.gen.loc[gidx, 'vm_pu'] = net.res_bus.loc[genbus, 'vm_pu']                       # SET THIS NETWORK GENS VREG TO OPF RESULTS
    #         for shkey in swsh_dict:                                                                 # LOOP ACROSS SWSHUNT KEYS
    #             shidx = swsh_dict[shkey]                                                            # GET SWSHUNT INDEX
    #             shbus = swshbus_dict[shidx]                                                         # GET SWSHUNT BUS
    #             net.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']                       # SET THIS NETWORK SWSHUNT VREG TO OPF RESULTS
    #         pp.runpp(net, enforce_q_lims=True)                                                      # RUN POWERFLOW
    #
    #         if o_type == 'GEN':                                                                     # IF OUTAGE WAS A GENERATOR...
    #             net.gen.in_service[o_idx] = True                                                    # PUT GENERATOR BACK IN-SERVICE
    #         if o_type == 'LINE':                                                                    # IF OUTAGE WAS A LINE...
    #             net.line.in_service[o_idx] = True                                                   # PUT LINE BACK IN-SERVICE
    #         if o_type == 'XFMR':                                                                    # IF OUTAGE WAS A XFMR...
    #             net.trafo.in_service[o_idx] = True                                                  # PUT XFMR BACK IN-SERVICE
    #         pp.runpp(net, init='results', enforce_q_lims=True)                                      # RUN POWERFLOW
    #
    #         previous_voltage_penalties = list(voltage_penalties)                                    # COPY OUTAGE PENALTIES BEFORE RUNNING OUTAGES AGAIN
    #         nosolve, solved_outages, nosolve_outages = get_nosolves(net, goutagekeys, boutagekeys, gen_dict, line_dict, xfmr_dict, voltage_step)
    #         constraint, constrained_outages, penalty = get_voltage_contrained_outages(net, solved_outages, online_gens, gen_dict, line_dict, xfmr_dict, bus_dict, voltage_step)
    #
    #         voltage_penalties.append(penalty)                                                       # ADD LOADING penalty TO penalties LIST
    #         voltage_penalties = list(set(voltage_penalties))                                        # REMOVE DUPLICATES FROM penalties LIST
    #         voltage_penalties.sort()                                                                # SORT LOADING penalties LIST
    #         if voltage_penalties == previous_voltage_penalties and penalty == min(voltage_penalties):   # IF LIST REPEATS AND penalty IS MINIMUM...
    #             stop = True                                                                             # STOP LOOPING
    #     except:                                                                                         # IF NO OPF SOLUTION...
    #         print(o_type, '{0:9s} DID NOT SOLVE WITH OPF ..............................'.format(o_key))     # PRINT OUTAGE INFO
    #         net = previous_net                                                                              # LOAD PRE-OUTAGE NETWORK
    #     voltage_step += 1

    # ---------------------------------------------------------------------------------------------
    # -- CREATE FINAL SCOPF NETWORKS --------------------------------------------------------------
    # ---------------------------------------------------------------------------------------------
    print('------------------ CREATING FINAL SCOPF NETWORKS -------------------')

    # -- COPY SCOPF RESULTS TO BASECASE AND CONTINGENCY NETWORKS ----------------------------------
    net.gen['p_mw'] = net.res_gen['p_mw']                                                           # SET THIS NETWORK GENERATORS POWER TO SCOPF RESULTS
    net_a.gen['p_mw'] = net.res_gen['p_mw']                                                         # SET THIS NETWORK GENERATORS POWER TO SCOPF RESULTS
    net_c.gen['p_mw'] = net.res_gen['p_mw']                                                         # SET THIS NETWORK GENERATORS POWER TO SCOPF RESULTS
    for gkey in gen_dict:                                                                           # LOOP ACROSS GENERATOR KEYS
        gidx = gen_dict[gkey]                                                                       # GET GENERATOR INDEX
        genbus = genbus_dict[gidx]                                                                  # GET GENERATOR BUS
        if genbus == swingbus:                                                                      # CHECK IF SWING BUS...
            continue                                                                                # IF SWING BUS, GET NEXT GENERATOR
        net.gen.loc[gidx, 'vm_pu'] = net.res_bus.loc[genbus, 'vm_pu']                               # SET THIS NETWORK GENS VREG TO SCOPF RESULTS
        net_a.gen.loc[gidx, 'vm_pu'] = net.res_bus.loc[genbus, 'vm_pu']                             # SET THIS NETWORK GENS VREG TO SCOPF RESULTS
        net_c.gen.loc[gidx, 'vm_pu'] = net.res_bus.loc[genbus, 'vm_pu']                             # SET THIS NETWORK GENS VREG TO SCOPF RESULTS
    for shkey in swsh_dict:                                                                         # LOOP ACROSS SWSHUNT KEYS
        shidx = swsh_dict[shkey]                                                                    # GET SWSHUNT INDEX
        shbus = swshbus_dict[shidx]                                                                 # GET SWSHUNT BUS
        net.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']                               # SET THIS NETWORK SWSHUNT VREG TO SCOPF RESULTS
        net_a.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']                             # SET THIS NETWORK SWSHUNT VREG TO SCOPF RESULTS
        net_c.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']                             # SET THIS NETWORK SWSHUNT VREG TO SCOPF RESULTS
    pp.runpp(net, enforce_q_lims=True)                                                              # THIS NETWORK, RUN STRAIGHT POWER FLOW
    pp.runpp(net_a, enforce_q_lims=True)                                                            # THIS NETWORK, RUN STRAIGHT POWER FLOW
    pp.runpp(net_c, enforce_q_lims=True)                                                            # THIS NETWORK, RUN STRAIGHT POWER FLOW

    # -- FINAL ATTEMPT AT BETTER ESTIMATE FOR SWING VREG ------------------------------------------
    nlevel_buses_v = [x for x in net.res_bus.loc[nlevel_buses, 'vm_pu']]                            # GET LIST OF NLEVEL BUS VOLTAGES
    max_v = max(nlevel_buses_v)                                                                     # GET MAX VOLTAGE OF NLEVEL BUSES
    net.gen.loc[swinggen_idxs, 'vm_pu'] = max_v                                                     # SET SWING GENS VREG = MAX OF NLEVEL BUSES VOLTAGE
    net.ext_grid.loc[ext_grid_idx, 'vm_pu'] = max_v                                                 # SET EXTGRID VREG = MAX OF NLEVEL BUSES VOLTAGE
    net_a.gen.loc[swinggen_idxs, 'vm_pu'] = max_v                                                   # SET SWING GENS VREG = MAX OF NLEVEL BUSES VOLTAGE
    net_a.ext_grid.loc[ext_grid_idx, 'vm_pu'] = max_v                                               # SET EXTGRID VREG = MAX OF NLEVEL BUSES VOLTAGE
    net_c.gen.loc[swinggen_idxs, 'vm_pu'] = max_v                                                   # SET SWING GENS VREG = MAX OF NLEVEL BUSES VOLTAGE
    net_c.ext_grid.loc[ext_grid_idx, 'vm_pu'] = max_v                                               # SET EXTGRID VREG = MAX OF NLEVEL BUSES VOLTAGE
    pp.runpp(net, enforce_q_lims=True)                                                              # THIS NETWORK, RUN STRAIGHT POWER FLOW
    pp.runpp(net_a, enforce_q_lims=True)                                                            # THIS NETWORK, RUN STRAIGHT POWER FLOW
    pp.runpp(net_c, enforce_q_lims=True)                                                            # THIS NETWORK, RUN STRAIGHT POWER FLOW

    # -- INSURE GENERATORS ARE MEETING VOLTAGE SCHEDULE -------------------------------------------
    for gkey in gen_dict:                                                                           # LOOP ACROSS GENERATOR KEYS
        gidx = gen_dict[gkey]                                                                       # GET GENERATOR INDEX
        genbus = genbus_dict[gidx]                                                                  # GET GENERATOR BUS
        qgen = net.res_gen.loc[gidx, 'q_mvar']                                                      # THIS GENERATORS QGEN
        qmin = net.gen.loc[gidx, 'min_q_mvar']                                                      # THIS GENERATORS QMIN
        qmax = net.gen.loc[gidx, 'max_q_mvar']                                                      # THIS GENERATORS QMAX
        bus_voltage = net.res_bus.loc[genbus, 'vm_pu']                                              # THIS GENERATORS BUS VOLTAGE
        if genbus == swingbus:                                                                      # CHECK IF SWING BUS...
            continue                                                                                # IF SWING BUS, GET NEXT GENERATOR
        if qgen == qmin or qgen == qmax:                                                            # IF THIS GENERATOR AT +/- QLIMIT...
            net.gen.loc[gidx, 'vm_pu'] = bus_voltage                                                # THIS NETWORK, SET THIS GENERATORS VREG TO BUS VOLTAGE
            net_a.gen.loc[gidx, 'vm_pu'] = bus_voltage                                              # THIS NETWORK, SET THIS GENERATORS VREG TO BUS VOLTAGE
            net_c.gen.loc[gidx, 'vm_pu'] = bus_voltage                                              # THIS NETWORK, SET THIS GENERATORS VREG TO BUS VOLTAGE
            pp.runpp(net, init='results', enforce_q_lims=True)                                      # THIS NETWORK, RUN STRAIGHT POWER FLOW
            pp.runpp(net_a, init='results', enforce_q_lims=True)                                    # THIS NETWORK, RUN STRAIGHT POWER FLOW
            pp.runpp(net_c, init='results', enforce_q_lims=True)                                    # THIS NETWORK, RUN STRAIGHT POWER FLOW

    # -- INSURE SWSHUNTS SUSCEPTANCE IS WITHIN LIMITS IN BASECASE ---------------------------------
    # -- HOPE CONSERVATIVE ENOUGH TO HOLD UP WITH CONTINGENCIES -----------------------------------
    # net = copy.deepcopy(net_a)                                                                      # GET FRESH NETWORK
    # pp.runpp(net, enforce_q_lims=True)                                                              # THIS NETWORK, RUN STRAIGHT POWER FLOW
    for shkey in swsh_dict:                                                                         # LOOP ACROSS SWSHUNT KEYS
        shidx = swsh_dict[shkey]                                                                    # GET SWSHUNT INDEX
        shbus = swshbus_dict[shidx]                                                                 # GET SWSHUNT BUS
        qgen = net.res_gen.loc[shidx, 'q_mvar']
        qmin = net.gen.loc[shidx, 'min_q_mvar']                                                     # GET MINIMUM SWSHUNT REACTIVE CAPABILITY
        qmax = net.gen.loc[shidx, 'max_q_mvar']                                                     # GET MAXIMUM SWSHUNT REACTIVE CAPABILITY
        voltage = net.res_bus.loc[shbus, 'vm_pu']                                                   # GET SWSHUNT BUS VOLTAGE
        if voltage < 1.0:                                                                           # IF BUS VOLTAGE IS < 1.0 (SUSCEPTANCE COULD BE EXCEEDED)
            if qgen / voltage ** 2 < 0.98 * qmin < 0.0:                                             # CHECK IF QMIN IS NEGATIVE AND SUSCEPTANCE OUT OF BOUNDS
                new_qmin = min(qmax, 0.99 * qmin * voltage ** 2)                                    # CALCULATE QMIN THAT IS IN BOUNDS
                net.gen.loc[shidx, 'min_q_mvar'] = new_qmin                                         # ADJUST QMIN IN POSITIVE DIRECTION WITH SOME EXTRA
                net_a.gen.loc[shidx, 'min_q_mvar'] = new_qmin                                       # ADJUST QMIN IN POSITIVE DIRECTION WITH SOME EXTRA
                net_c.gen.loc[shidx, 'min_q_mvar'] = new_qmin                                       # ADJUST QMIN IN POSITIVE DIRECTION WITH SOME EXTRA
                # print(shkey, 'Adj QMIN Up', 'QMIN =', qmin, 'NEW QMIN =', new_qmin)               # DEVELOPEMENT... PRINT MESSAGE
            elif qgen / voltage ** 2 > 0.98 * qmax > 0.0:                                           # CHECK IF QMAX IS NEGATIVE AND SUSCEPTANCE OUT OF BOUNDS
                new_qmax = max(qmin, 0.99 * qmax * voltage ** 2)                                    # CALCULATE QMAX THAT IS IN BOUNDS
                net.gen.loc[shidx, 'max_q_mvar'] = new_qmax                                         # ADJUST QMAX IN NEGATIVE DIRECTION WITH SOME EXTRA
                net_a.gen.loc[shidx, 'max_q_mvar'] = new_qmax                                       # ADJUST QMAX IN NEGATIVE DIRECTION WITH SOME EXTRA
                net_c.gen.loc[shidx, 'max_q_mvar'] = new_qmax                                       # ADJUST QMAX IN NEGATIVE DIRECTION WITH SOME EXTRA
                # print(shkey, 'Adj QMAX Down', 'QMAX =', qmax, 'NEW QMAX =', new_qmax)             # DEVELOPEMENT... PRINT MESSAGE
            pp.runpp(net, init='results', enforce_q_lims=True)                                      # THIS NETWORK, RUN STRAIGHT POWER FLOW
            pp.runpp(net_a, init='results', enforce_q_lims=True)                                    # THIS NETWORK, RUN STRAIGHT POWER FLOW
            pp.runpp(net_c, init='results', enforce_q_lims=True)                                    # THIS NETWORK, RUN STRAIGHT POWER FLOW

    external_pgen = net_a.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                    # GET EXTERNAL GRID REAL POWER
    external_qgen = net_a.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                  # GET EXTERNAL GRID REACTIVE POWER

    # ---------------------------------------------------------------------------------------------
    # -- ZERO OUT EXTERNAL GRID REAL AND REACTIVE POWER (IF NEEDED)  ------------------------------
    # ---------------------------------------------------------------------------------------------
    net = copy.deepcopy(net_a)
    external_pgen_threshold = 1e-4                                                                  # THRESHOLD FOR ZEROING OUT BASECASE EXTERNAL PGEN
    external_qgen_threshold = 1e-4                                                                  # THRESHOLD FOR ZEROING OUT BASECASE EXTERNAL QGEN
    zeroed = True                                                                                   # INITIALIZE ZEROED FLAG
    if abs(external_pgen) > external_pgen_threshold:                                                # IF EXTERNAL REAL POWER > THRESHOLD...
        zeroed = False                                                                              # SET ZEROED FLAG = FALSE
    if abs(external_qgen) > external_qgen_threshold:                                                # IF EXTERNAL REACTIVE POWER > THRESHOLD...
        zeroed = False                                                                              # SET ZEROED FLAG = FALSE
    step = 1                                                                                        # INITIALIZE ITERATOR
    while not zeroed and step < 120:                                                                # LIMIT WHILE LOOP ITERATIONS
        zeroed = True                                                                               # SET ZEROED FLAG = TRUE
        # -- CALCULATE PARTICIPATING GENERATORS UP-DOWN P AND Q MARGINS -------
        p_upmargin_total = 0.0                                                                      # INITIALIZE TOTAL P-UP MARGIN
        p_downmargin_total = 0.0                                                                    # INITIALIZE TOTAL P-DOWN MARGIN
        p_upmargin_dict = {}                                                                        # INITIALIZE P-UP MARGIN DICT
        p_downmargin_dict = {}                                                                      # INITIALIZE P-DOWN MARGIN DICT
        q_upmargin_total = 0.0                                                                      # INITIALIZE TOTAL Q-UP MARGIN
        q_downmargin_total = 0.0                                                                    # INITIALIZE TOTAL Q-DOWN MARGIN
        q_upmargin_dict = {}                                                                        # INITIALIZE Q-UP MARGIN DICT
        q_downmargin_dict = {}                                                                      # INITIALIZE Q-DOWN MARGIN DICT
        for gkey in participating_gens:                                                             # LOOP THROUGH PARTICIPATING GENERATORS
            gidx = gen_dict[gkey]                                                                   # GET THIS PARTICIPATING GENERATOR INDEX
            pgen = net.res_gen.loc[gidx, 'p_mw']                                                    # THIS GENERATORS PGEN
            pmin = net.gen.loc[gidx, 'min_p_mw']                                                    # THIS GENERATORS PMIN
            pmax = net.gen.loc[gidx, 'max_p_mw']                                                    # THIS GENERATORS PMAX
            p_upmargin = pmax - pgen                                                                # THIS GENERATORS P-UP MARGIN
            p_downmargin = pgen - pmin                                                              # THIS GENERATORS P-DOWN MARGIN
            p_upmargin_dict.update({gidx: p_upmargin})                                              # UPDATE P-UP MARGIN DICT
            p_upmargin_total += p_upmargin                                                          # INCREMENT TOTAL P-UP MARGIN
            p_downmargin_dict.update({gidx: p_downmargin})                                          # UPDATE P-DOWN MARGIN DICT
            p_downmargin_total += p_downmargin                                                      # INCREMENT TOTAL P-DOWN MARGIN
            qgen = net.res_gen.loc[gidx, 'q_mvar']                                                  # THIS GENERATORS QGEN
            qmin = net.gen.loc[gidx, 'min_q_mvar']                                                  # THIS GENERATORS QMIN
            qmax = net.gen.loc[gidx, 'max_q_mvar']                                                  # THIS GENERATORS QMAX
            q_upmargin = qmax - qgen                                                                # THIS GENERATORS Q-UP MARGIN
            q_downmargin = qgen - qmin                                                              # THIS GENERATORS Q-DOWN MARGIN
            q_upmargin_dict.update({gidx: q_upmargin})                                              # UPDATE Q-UP MARGIN DICT
            q_upmargin_total += q_upmargin                                                          # INCREMENT TOTAL Q-UP MARGIN
            q_downmargin_dict.update({gidx: q_downmargin})                                          # UPDATE Q-DOWN MARGIN DICT
            q_downmargin_total += q_downmargin                                                      # INCREMENT TOTAL Q-DOWN MARGIN
        if abs(external_pgen) > external_pgen_threshold:                                            # CHECK IF EXTERNAL REAL POWER EXCEED THRESHOLD
            for gkey in participating_gens:                                                         # LOOP THROUGH PARTICIPATING GENERATORS
                gidx = gen_dict[gkey]                                                               # GET THIS PARTICIPATING GENERATOR INDEX
                zeroed = False                                                                      # SET ZEROED FLAG
                pgen = net.res_gen.loc[gidx, 'p_mw']                                                # THIS GENERATORS REAL POWER
                if external_pgen < -external_pgen_threshold:                                        # CHECK IF EXTERNAL REAL POWER IS NEGATIVE
                    p_downmargin = p_downmargin_dict[gidx]                                          # GET THIS GENERATORS P-DOWN MARGIN
                    if p_downmargin < 1.0:                                                          # IF NO MARGIN...
                        continue                                                                    # GET NEXT GENERATOR
                    delta_pgen = external_pgen * p_downmargin_dict[gidx] / p_downmargin_total       # CALCULATE GENERATOR INCREMENT (DISTRIBUTED PROPORTIONALLY)
                    new_pgen = pgen + delta_pgen                                                    # CALCULATE  GENERATOR NEXT PGEN
                    net.gen.loc[gidx, 'p_mw'] = new_pgen                                            # SET GENERATOR PGEN FOR THIS NETWORK
                    net_a.gen.loc[gidx, 'p_mw'] = new_pgen                                          # SET GENERATOR PGEN FOR THIS NETWORK
                    net_c.gen.loc[gidx, 'p_mw'] = new_pgen                                          # SET GENERATOR PGEN FOR THIS NETWORK
                if external_pgen > external_pgen_threshold:                                         # CHECK IF EXTERNAL REAL POWER IS POSITIVE
                    p_upmargin = p_upmargin_dict[gidx]                                              # GET THIS GENERATORS P-UP MARGIN
                    if p_upmargin < 1.0:                                                            # IF NO MARGIN...
                        continue                                                                    # GET NEXT GENERATOR
                    delta_pgen = external_pgen * p_upmargin / p_upmargin_total                      # CALCULATE GENERATOR INCREMENT (DISTRIBUTED PROPORTIONALLY)
                    new_pgen = pgen + delta_pgen                                                    # CALCULATE  GENERATOR NEXT PGEN
                    net.gen.loc[gidx, 'p_mw'] = new_pgen                                            # SET GENERATOR PGEN FOR THIS NETWORK
                    net_a.gen.loc[gidx, 'p_mw'] = new_pgen                                          # SET GENERATOR PGEN FOR THIS NETWORK
                    net_c.gen.loc[gidx, 'p_mw'] = new_pgen                                          # SET GENERATOR PGEN FOR THIS NETWORK
        if abs(external_qgen) > external_qgen_threshold:                                            # CHECK IF EXTERNAL REACTIVE POWER EXCEED THRESHOLD
            for gkey in participating_gens:                                                         # LOOP THROUGH PARTICIPATING GENERATORS
                gidx = gen_dict[gkey]                                                               # GET THIS PARTICIPATING GENERATOR INDEX
                zeroed = False                                                                      # SET ZEROED FLAG
                if gidx in swinggen_idxs:                                                           # CHECK IF SWING GENERATOR...
                    continue                                                                        # IF SWING GEN, GET NEXT GENERATOR
                vreg = net.res_gen.loc[gidx, 'vm_pu']                                               # THIS GENERATORS VOLTAGE SETPOINT
                if external_qgen < -external_qgen_threshold:                                        # CHECK IF EXTERNAL REACTIVE POWER IS NEGATIVE
                    q_downmargin = q_downmargin_dict[gidx]                                          # GET THIS GENERATORS Q-DOWN MARGIN
                    if vreg < 0.901 or q_downmargin < 1.0:                                          # IF NO MARGIN, OR BUS VOLTAGE IS LOW...
                        continue                                                                        # IF SO, GET NEXT GENERATOR
                    delta_vreg = 0.020 * external_qgen * q_downmargin_dict[gidx] / q_downmargin_total   # CALCULATE SETPOINT INCREMENT (PROPORTIONAL)
                    new_vreg = vreg + delta_vreg                                                        # CALCULATE NEW SET POINT
                    net.gen.loc[gidx, 'vm_pu'] = new_vreg                                           # SET GENERATOR QGEN FOR THIS NETWORK
                    net_a.gen.loc[gidx, 'vm_pu'] = new_vreg                                         # SET GENERATOR QGEN FOR THIS NETWORK
                    net_c.gen.loc[gidx, 'vm_pu'] = new_vreg                                         # SET GENERATOR QGEN FOR THIS NETWORK
                if external_qgen > external_qgen_threshold:                                         # CHECK IF EXTERNAL REACTIVE POWER IS POSITIVE
                    q_upmargin = q_upmargin_dict[gidx]                                              # GET THIS GENERATORS Q-UP MARGIN
                    if vreg > 1.099 or q_upmargin < 1.0:                                            # IF NO MARGIN, OR BUS VOLTAGE IS HIGH...
                        continue                                                                    # IF SO, GET NEXT GENERATOR
                    delta_vreg = 0.020 * external_qgen * q_upmargin_dict[gidx] / q_upmargin_total   # CALCULATE SETPOINT INCREMENT (DISTRIBUTED PROPORTIONALLY)
                    new_vreg = vreg + delta_vreg                                                    # CALCULATE NEW SET POINT
                    net.gen.loc[gidx, 'vm_pu'] = new_vreg                                           # SET GENERATOR QGEN FOR THIS NETWORK
                    net_a.gen.loc[gidx, 'vm_pu'] = new_vreg                                         # SET GENERATOR QGEN FOR THIS NETWORK
                    net_c.gen.loc[gidx, 'vm_pu'] = new_vreg                                         # SET GENERATOR QGEN FOR THIS NETWORK
        pp.runpp(net, enforce_q_lims=True)                                                          # RUN STRAIGHT POWER FLOW ON THIS NETWORK
        external_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                  # GET EXTERNAL GRID REAL POWER
        external_qgen = net.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                # GET EXTERNAL GRID REACTIVE POWER
        step += 1                                                                                   # INCREMENT ITERATOR
    pp.runpp(net_c, enforce_q_lims=True)                                                            # RUN STRAIGHT POWER FLOW ON CONTINGENCY BASECASE
    pp.runpp(net_a, enforce_q_lims=True)                                                            # RUN STRAIGHT POWER FLOW ON BASECASE
    ex_pgen = net_a.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                          # GET EXTERNAL GRID REAL POWER
    ex_qgen = net_a.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                        # GET EXTERNAL GRID REACTIVE POWER
    base_cost = get_generation_cost(net_a, participating_gens, gen_dict, pwlcost_dict0)             # GET TOTAL COST OF GENERATION
    print('FINAL SCOPF NETWORKS CREATED ........................ ${0:6.0f} ......'.format(base_cost), round(time.time() - opf_startime, 3), 'SECONDS')

    # -- GET BASECASE GENERATORS POWER OUTPUT -----------------------------------------------------
    base_pgens = {}                                                                                 # INITIALIZE BASECASE GENERATOR POWER DICT
    for gkey in participating_gens:                                                                 # LOOP ACROSS PARTICIPATING GENERATOR KEYS
        gidx = gen_dict[gkey]                                                                       # GET GENERATOR INDEX
        base_pgen = net_a.res_gen.loc[gidx, 'p_mw']                                                 # GET THIS GENERATOR POWER OUTPUT
        base_pgens.update({gkey: base_pgen})                                                        # UPDATE BASECASE GENERATOR POWER DICT

    # ---------------------------------------------------------------------------------------------
    # -- WRITE BASECASE BUS AND GENERATOR RESULTS TO FILE -----------------------------------------
    # ---------------------------------------------------------------------------------------------
    print()
    print('WRITING BASECASE RESULTS TO FILE .... {0:.5f} MW {1:.5f} MVAR ......'.format(ex_pgen + 0.0, ex_qgen + 0.0))
    bus_results = copy.deepcopy(net_a.res_bus)                                                      # GET BASECASE BUS RESULTS
    gen_results = copy.deepcopy(net_a.res_gen)                                                      # GET BASECASE GENERATOR RESULTS
    write_base_bus_results(outfname1, bus_results, swshidx_dict, gen_results, ext_grid_idx)         # WRITE SOLUTION1 BUS RESULTS
    write_base_gen_results(outfname1, gen_results, Gids, genbuses, swshidxs)                        # WRITE SOLUTION1 GEN RESULTS

    # =============================================================================================
    # -- WRITE MISC DATA TO FILE (READ IN MYPYTHON2) ----------------------------------------------
    # =============================================================================================
    # write_starttime = time.time()
    # pp.to_pickle(net_a, neta_fname)                                                                 # WRITE BASECASE RATEA NETWORK TO FILE
    # pp.to_pickle(net_c, netc_fname)                                                                 # WRITE CONTINGENCY RATEC NETWORK TO FILE
    # PFile = open(data_fname, 'wb')                                                                  # OPEN PICKLE FILE
    # pickle.dump([base_pgens, ext_grid_idx, genbuses, genbus_dict, Gids, line_dict, goutagekeys,
    #              boutagekeys, outage_dict, gen_dict, pfactor_dict, xfmr_dict, area_swhunts,
    #              swsh_dict, swshbus_dict, swshidxs, swshidx_dict, busarea_dict, branch_areas,
    #              area_participating_gens, swinggen_idxs, alt_sw_genkey, tie_idx, alt_tie_idx], PFile)   # WRITE MISC DATA TO PICKLE FILE
    # PFile.close()                                                                                       # CLOSE PICKLE FILE
    #
    # print('WRITING DATA TO FILE -----------------------------------------------', round(time.time() - write_starttime, 3))

    print('DONE ---------------------------------------------------------------')
    print('TOTAL TIME -------------------------------------------------------->', round(time.time() - start_time, 3))
