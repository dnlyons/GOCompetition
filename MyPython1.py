import os
import sys
import csv
import math
import time
import copy
import numpy
import pandapower as pp
from pandas import options as pdoptions

cwd = os.path.dirname(__file__)

# -- USING .INI FILE ----------------------------------------------------------
# from configparser import ConfigParser
# if not sys.argv[1:]:
#     config = ConfigParser()
#     config.read(os.path.join(cwd, 'sandbox', 'inputfiles.ini'))
#     rop_fname = cwd + config.get('INPUTS', 'ROP')
#     raw_fname = cwd + config.get('INPUTS', 'RAW')
#     con_fname = cwd + config.get('INPUTS', 'CON')
#     inl_fname = cwd + config.get('INPUTS', 'INL')
#     outfname1 = cwd + r'/sandbox/scenario_1/solution1.txt'
#     outfname2 = cwd + r'/sandbox/scenario_1/solution2.txt'

# -- DEVELOPMENT DEFAULT ------------------------------------------------------
if not sys.argv[1:]:
    rop_fname = cwd + r'/sandbox/scenario_1/case.rop'
    raw_fname = cwd + r'/sandbox/scenario_1/case.raw'
    con_fname = cwd + r'/sandbox/scenario_1/case.con'
    inl_fname = cwd + r'/sandbox/scenario_1/case.inl'
    outfname1 = cwd + r'/sandbox/scenario_1/solution1.txt'
    outfname2 = cwd + r'/sandbox/scenario_1/solution2.txt'

# -- USING COMMAND LINE -------------------------------------------------------
if sys.argv[1:]:
    print()
    raw_fname = sys.argv[1]
    rop_fname = sys.argv[2]
    con_fname = sys.argv[3]
    inl_fname = sys.argv[4]
    outfname1 = sys.argv[5]
    outfname2 = sys.argv[6]


CONRATING = 2       # contingency line and xfmr ratings 0=RateA, 1=RateB, 2=RateC
ITMXN = 40          # max iterations solve option
RUN_OPF = 1         # 0=normal powerflow, 1=optimal powerflow


# =============================================================================
# -- FUNCTIONS ----------------------------------------------------------------
# =============================================================================
def get_raw_csvdata(fname):
    with open(fname, 'rU') as fobject:
        reader = csv.reader(fobject, delimiter=',', quotechar="'")
        for row in reader:
            row = [x.strip() for x in row]
            yield row
    fobject.close()
    return


def get_con_csvdata(fname):
    with open(fname, 'rU') as fobject:
        reader = csv.reader(fobject, delimiter=' ', quotechar="'", skipinitialspace=True)
        for row in reader:
            row = [x.strip() for x in row]
            yield row
    fobject.close()
    return


def get_reserve_csvdata(fname):
    with open(fname, 'rU') as fobject:
        reader = csv.reader(fobject, delimiter=',', quotechar="'", skipinitialspace=True)
        for row in reader:
            row = [x.strip() for x in row]
            yield row
    fobject.close()
    return


def get_contingencies(fname):
    condict = {'branch': {}, 'gen': {}}
    dobj = get_con_csvdata(fname)
    while True:
        line = next(dobj)
        if not line:
            continue
        if line[0].upper() == 'END':
            break
        if line[0].upper() == 'CONTINGENCY':
            clabel = line[1]
            while True:
                line = next(dobj)
                if not line:
                    continue
                if line[0].upper() == 'END':
                    break
                if len(line) == 10:
                    bkey = line[4] + '-' + line[7] + '-' + line[9]
                    condict['branch'].update({clabel: bkey})
                if len(line) == 6:
                    gkey = line[5] + '-' + line[2]
                    condict['gen'].update({clabel: gkey})
    return condict


def get_gen_reserves(fname):
    pfdict = {}
    dobj = get_reserve_csvdata(fname)
    while True:
        line = next(dobj)
        if not line:
            continue
        if line[0] == '0':
            break
        gkey = line[0] + '-' + line[1]
        pfdict.update({gkey: -1e3 * float(line[5])})
    return pfdict


def format_busdata(lol):
    areanums = []
    if lol != [[]]:
        for i in range(len(lol)):
            lol[i] = [int(lol[i][0]), str(lol[i][1]), float(lol[i][2]), int(lol[i][3]), int(lol[i][4]), int(lol[i][5]), int(lol[i][6]),
                      float(lol[i][7]), float(lol[i][8]), float(lol[i][9]), float(lol[i][10]), float(lol[i][11]), float(lol[i][12])]
            areanums.append(lol[i][3])
            areanums = list(set(areanums))
            areanums.sort()
    return lol, areanums


def format_loaddata(lol):
    # load = ['I', 'ID', 'STATUS', 'AREA', 'ZONE', 'PL', 'QL', 'IP', 'IQ', 'YP', 'YQ', 'OWNER', 'SCALE', 'INTRPT']
    if lol != [[]]:
        for i in range(len(lol)):
            lol[i] = [int(lol[i][0]), str(lol[i][1]), int(lol[i][2]), int(lol[i][3]), int(lol[i][4]), float(lol[i][5]), float(lol[i][6]),
                      float(lol[i][7]), float(lol[i][8]), float(lol[i][9]), float(lol[i][10]), int(lol[i][11]), int(lol[i][12]), int(lol[i][13])]
    return lol


def format_fixshuntdata(lol):
    # fixshunt = ['I', 'ID', 'STATUS', 'GL', 'BL']
    if lol != [[]]:
        for i in range(len(lol)):
            lol[i] = [int(lol[i][0]), str(lol[i][1]), int(lol[i][2]), float(lol[i][3]), float(lol[i][4])]
    return lol


def format_gendata(lol):
    # gens = ['I', 'ID', 'PG', 'QG', 'QT', 'QB', 'VS', 'IREG', 'MBASE', 'ZR', 'ZX', 'RT', 'XT', 'GTAP', 'STAT', 'RMPCT', 'PT', 'PB',
    #         'O1', 'F1', 'O2', 'F2', 'O3', 'F3', 'O4', 'F4', 'WMOD', 'WPF']
    if lol != [[]]:
        for i in range(len(lol)):
            lol[i] = [int(lol[i][0]), str(lol[i][1]),
                      round(-1e3 * float(lol[i][2]), 5), round(-1e3 * float(lol[i][3]), 5),
                      round(-1e3 * float(lol[i][4]), 5), round(-1e3 * float(lol[i][5]), 5),
                      float(lol[i][6]), int(lol[i][7]), float(lol[i][8]), float(lol[i][9]), float(lol[i][10]),
                      float(lol[i][11]), float(lol[i][12]), float(lol[i][13]), int(lol[i][14]), float(lol[i][15]),
                      round(-1e3 * float(lol[i][16]), 5),
                      round(-1e3 * float(lol[i][17]), 5),
                      int(lol[i][18]), float(lol[i][19]),
                      int(lol[i][20]), float(lol[i][21]),
                      int(lol[i][22]), float(lol[i][23]),
                      int(lol[i][24]), float(lol[i][25]),
                      int(lol[i][26]), int(lol[i][27])]
    return lol


def format_branchdata(lol):
    # branch = ['I', 'J', 'CKT', 'R', 'X', 'B', 'RATEA', 'RATEB', 'RATEC', 'GI', 'BI', 'GJ', 'BJ', 'ST', 'MET', 'LEN',
    #           'O1', 'F1', 'O2', 'F2', 'O3', 'F3', 'O4', 'F4']
    if lol != [[]]:
        for i in range(len(lol)):
            lol[i] = [int(lol[i][0]), int(lol[i][1]), str(lol[i][2]), float(lol[i][3]), float(lol[i][4]), float(lol[i][5]), float(lol[i][6]),
                      float(lol[i][7]), float(lol[i][8]), float(lol[i][9]), float(lol[i][10]), float(lol[i][11]), float(lol[i][12]), int(lol[i][13]),
                      int(lol[i][14]), float(lol[i][15]), int(lol[i][16]), float(lol[i][17]), int(lol[i][18]), float(lol[i][19]),
                      int(lol[i][20]), float(lol[i][21]), int(lol[i][22]), float(lol[i][23])]
    return lol


def split_xfmrdata(lol):
    xfmrdata2w = []
    xfmrdata3w = []
    i = 0
    while i < len(lol):
        if lol[i][2] == '0':
            xfmrdata2w.append(lol[i + 0] + lol[i + 1] + lol[i + 2] + lol[i + 3])
            i += 4
        elif lol[i][2] != '0':
            xfmrdata3w.append(lol[i + 0] + lol[i + 1] + lol[i + 2] + lol[i + 3] + lol[i + 4])
            i += 5
    return xfmrdata2w, xfmrdata3w


def format_xfmr2wdata(lol):
    # 2wxfmr = ['I', 'J', 'K', 'CKT', 'CW', 'CZ', 'CM', 'MAG1', 'MAG2', 'NMETR', 'NAME', 'STAT', 'O1', 'F1', 'O2', 'F2', 'O3', 'F3', 'O4', 'F4', 'VECGRP',
    #           'R1-2', 'X1-2', 'SBASE1-2', 'WINDV1', 'NOMV1', 'ANG1', 'RATA1', 'RATB1', 'RATC1', 'COD1', 'CONT1', 'RMA1', 'RMI1', 'VMA1', 'VMI1',
    #           'NTP1', 'TAB1', 'CR1', 'CX1', 'CNXA1', 'WINDV2', 'NOMV2']
    if lol != [[]]:
        for i in range(len(lol)):
            lol[i] = [
                # -- record1 --------------------------------------------------
                int(lol[i][0]), int(lol[i][1]), int(lol[i][2]), str(lol[i][3]),
                int(lol[i][4]), int(lol[i][5]), int(lol[i][6]),
                float(lol[i][7]), float(lol[i][8]), int(lol[i][9]),
                str(lol[i][10]), int(lol[i][11]),  # status is end
                int(lol[i][12]), float(lol[i][13]),  # o1 f1
                int(lol[i][14]), float(lol[i][15]),  # o2 f2
                int(lol[i][16]), float(lol[i][17]),  # o3 f3
                int(lol[i][18]), float(lol[i][19]),  # o4 f4
                str(lol[i][20]),  # vecgroup
                # -- record2 --------------------------------------------------
                float(lol[i][21]), float(lol[i][22]), float(lol[i][23]),  # r1 x1 sbase1-2
                # -- record3 --------------------------------------------------
                float(lol[i][24]), float(lol[i][25]), float(lol[i][26]),  # windv1 nomv1 angle1
                float(lol[i][27]), float(lol[i][28]), float(lol[i][29]),  # rate1A rate1A rate1C
                int(lol[i][30]), int(lol[i][31]),  # cod1 cont1
                float(lol[i][32]), float(lol[i][33]),  # rma1 rmi1
                float(lol[i][34]), float(lol[i][35]),  # vma1 vmi1
                int(lol[i][36]), int(lol[i][37]),  # ntp1 tab1
                float(lol[i][38]), float(lol[i][39]), float(lol[i][40]),  # cr1 cx1 cnxa1
                # -- record4 --------------------------------------------------
                float(lol[i][41]), float(lol[i][42])  # windv2 nomv2
            ]
    return lol


def format_xfmr3wdata(lol):
    # 3wxfmr = ['I', 'J', 'K', 'CKT', 'CW', 'CZ', 'CM', 'MAG1', 'MAG2', 'NMETR', 'NAME', 'STAT', 'O1', 'F1', 'O2', 'F2', 'O3', 'F3', 'O4', 'F4', 'VECGRP',
    #           'R1 - 2', 'X1 - 2', 'SBASE1 - 2', 'R2 - 3', 'X2 - 3', 'SBASE2 - 3', 'R3 - 1', 'X3 - 1', 'SBASE3 - 1', 'VMSTAR', 'ANSTAR',
    #           'WINDV1', 'NOMV1', 'ANG1', 'RATA1', 'RATB1', 'RATC1', 'COD1', 'CONT1', 'RMA1', 'RMI1', 'VMA1', 'VMI1', 'NTP1', 'TAB1', 'CR1', 'CX1', 'CNXA1',
    #           'WINDV2', 'NOMV2', 'ANG2', 'RATA2', 'RATB2', 'RATC2', 'COD2', 'CONT2', 'RMA2', 'RMI2', 'VMA2', 'VMI2', 'NTP2', 'TAB2', 'CR2', 'CX2', 'CNXA2',
    #           'WINDV3', 'NOMV3', 'ANG3', 'RATA3', 'RATB3', 'RATC3', 'COD3', 'CONT3', 'RMA3', 'RMI3', 'VMA3', 'VMI3', 'NTP3', 'TAB3', 'CR3', 'CX3', 'CNXA3']
    if lol != [[]]:
        for i in range(len(lol)):
            lol[i] = [
                # -- record1 --------------------------------------------------
                int(lol[i][0]), int(lol[i][1]), int(lol[i][2]), str(lol[i][3]),
                int(lol[i][4]), int(lol[i][5]), int(lol[i][6]),
                float(lol[i][7]), float(lol[i][8]), int(lol[i][9]),
                str(lol[i][10]), int(lol[i][11]),  # status is end
                int(lol[i][12]), float(lol[i][13]),  # o1 f1
                int(lol[i][14]), float(lol[i][15]),  # o2 f2
                int(lol[i][16]), float(lol[i][17]),  # o3 f3
                int(lol[i][18]), float(lol[i][19]),  # o4 f4
                str(lol[i][20]),  # vecgroup
                # -- record2 --------------------------------------------------
                float(lol[i][21]), float(lol[i][22]), float(lol[i][23]),  # r1 x1 sbase1
                float(lol[i][24]), float(lol[i][25]), float(lol[i][26]),  # r2 x3 sbase2
                float(lol[i][27]), float(lol[i][28]), float(lol[i][29]),  # r3 x3 sbase3
                int(lol[i][30]), int(lol[i][31]),  # vmstar anstar
                # -- record3 --------------------------------------------------
                float(lol[i][32]), float(lol[i][33]), float(lol[i][34]),  # windv1 nomv1 angle1
                float(lol[i][35]), float(lol[i][36]), float(lol[i][37]),  # rate1A rate1A rate1C
                int(lol[i][38]), int(lol[i][39]),  # cod1 cont1
                float(lol[i][40]), float(lol[i][41]),  # rma1 rmi1
                float(lol[i][42]), float(lol[i][43]),  # vma1 vmi1
                int(lol[i][44]), int(lol[i][45]),  # ntp1 tab1
                float(lol[i][46]), float(lol[i][47]), float(lol[i][48]),  # cr1 cx1 cnxa1
                # -- record4 --------------------------------------------------
                float(lol[i][49]), float(lol[i][50]), float(lol[i][51]),  # windv2 nomv2 angle2
                float(lol[i][52]), float(lol[i][53]), float(lol[i][54]),  # rate2A rate2A rate2C
                int(lol[i][55]), int(lol[i][56]),  # cod2 cont2
                float(lol[i][57]), float(lol[i][58]),  # rma2 rmi2
                float(lol[i][59]), float(lol[i][60]),  # vma2 vmi2
                int(lol[i][61]), int(lol[i][62]),  # ntp2 tab2
                float(lol[i][63]), float(lol[i][64]), float(lol[i][65]),  # cr2 cx2 cnxa2
                # -- record5 --------------------------------------------------
                float(lol[i][66]), float(lol[i][67]), float(lol[i][68]),  # windv3 nomv3 angle3
                float(lol[i][69]), float(lol[i][70]), float(lol[i][71]),  # rate3A rate3A rate3C
                int(lol[i][72]), int(lol[i][73]),  # cod3 cont3
                float(lol[i][74]), float(lol[i][75]),  # rma3 rmi3
                float(lol[i][76]), float(lol[i][77]),  # vma3 vmi3
                int(lol[i][78]), int(lol[i][79]),  # ntp3 tab3
                float(lol[i][80]), float(lol[i][81]), float(lol[i][82])  # cr3 cx3 cnxa3
            ]
    return lol


def format_zonedata(lol):
    if lol != [[]]:
        for i in range(len(lol)):
            lol[i] = [int(lol[i][0]), str(lol[i][1])]
    return lol


def format_ownerdata(lol):
    if lol != [[]]:
        for i in range(len(lol)):
            lol[i] = [int(lol[i][0]), str(lol[i][1])]
    return lol


def format_swshuntdata(lol):
    # I, MODSW, ADJM, STAT, VSWHI, VSWLO, SWREM, RMPCT, RMIDNT, BINIT, N1, B1, N2, B2, N3, B3, N4, B4, N5, B5, N6, B6, N7, B7, N8, B8
    if lol != [[]]:
        for i in range(len(lol)):
            lol[i] = [int(lol[i][0]), int(lol[i][1]), int(lol[i][2]), int(lol[i][3]), float(lol[i][4]),
                      float(lol[i][5]), int(lol[i][6]), float(lol[i][7]), str(lol[i][8]), float(lol[i][9]),
                      int(lol[i][10]), float(lol[i][11]), int(lol[i][12]), float(lol[i][13]),
                      int(lol[i][14]), float(lol[i][15]), int(lol[i][16]), float(lol[i][17]),
                      int(lol[i][18]), float(lol[i][19]), int(lol[i][20]), float(lol[i][21]),
                      int(lol[i][22]), float(lol[i][23]), int(lol[i][24]), float(lol[i][25])]
    return lol


def get_swingbus_data(lol):
    # bus = ['I', 'NAME', 'BASKV', 'IDE', 'AREA', 'ZONE', 'OWNER', 'VM', 'VA', 'NVHI', 'NVLO', 'EVHI', 'EVLO']
    swbus = None
    swangle = 0.0
    for i in lol:
        if i[3] == 3:
            swbus = i[0]
            swname = i[1]
            swkv = i[2]
            swangle = i[8]
            swvhigh = i[11]
            swvlow = i[12]
            break
    return [swbus, swname, swkv, swangle, swvlow, swvhigh]


def get_swing_gen_data(lol, swbus):
    # gens = ['I', 'ID', 'PG', 'QG', 'QT', 'QB', 'VS', 'IREG', 'MBASE', 'ZR', 'ZX', 'RT', 'XT', 'GTAP', 'STAT', 'RMPCT', 'PT', 'PB',
    #         'O1', 'F1', 'O2', 'F2', 'O3', 'F3', 'O4', 'F4', 'WMOD', 'WPF']
    for i in lol:
        if i[0] == swbus:
            sw_key = str(i[0]) + '-' + i[1]
            sw_qmin = -1e3 * i[4]
            sw_qmax = -1e3 * i[5]
            vreg_sw = i[6]
            sw_pmin = -1e3 * i[16]
            sw_pmax = -1e3 * i[17]
            break
    return sw_key, vreg_sw, sw_qmin, sw_qmax, sw_pmin, sw_pmax


def write_csvdata(fname, lol, label):
    with open(fname, 'a', newline='') as fobject:
        writer = csv.writer(fobject, delimiter=',', quotechar='"')
        for j in label:
            writer.writerow(j)
        writer.writerows(lol)
    fobject.close()
    return


def write_base_bus_results(fname, b_results, f_dict, s_dict, g_results, sh_results, exgridbus):

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
    b_results.rename(columns={'vm_pu': 'voltage', 'va_degree': 'angle'}, inplace=True)
    # -- PREVENT NEGATIVE ZEROS -----------------------------------------------
    b_results['voltage'] += 0.0
    b_results['angle'] += 0.0
    # -- CONVERT PANDAS DATAFRAME TO LIST FOR REPORTING -----------------------
    buslist = [b_results.columns.values.tolist()] + b_results.values.tolist()
    # -- GET ANY SHUNT MVARS FOR REPORTING ------------------------------------
    # -- (SWITCHED SHUNTS ARE MODELED AS GENERATORS) --------------------------
    for j in range(1, len(buslist)):
        buslist[j][0] = int(buslist[j][0])
        bus = buslist[j][0]
        mvars = 0.0
        if bus in f_dict:
            mvars = -1e-3 * sh_results.loc[f_dict[bus], 'q_kvar']
        if bus in s_dict:
            mvars = -1e-3 * g_results.loc[s_dict[bus], 'q_kvar']
        if bus in f_dict and bus in s_dict:
            mvars1 = -1e-3 * sh_results.loc[f_dict[bus], 'q_kvar']
            mvars2 = -1e-3 * g_results.loc[s_dict[bus], 'q_kvar']
            mvars = mvars1 + mvars2
        buslist[j][3] = mvars + 0.0
    # -- WRITE THE BUS RESULTS TO FILE ----------------------------------------
    write_csvdata(fname, buslist, [['--bus section']])
    return b_results


def write_base_gen_results(fname, g_results, genids, gbuses, e_results, swbus, exgridbus):
    g_results.drop(g_results.index[sw_indexes], inplace=True)
    del g_results['vm_pu']
    del g_results['va_degree']
    # -- COMBINE SWING GENERATOR AND EXTERNAL GRID CONTRIBUTIONS --------------
    sw_kw = g_results.loc[swbus, 'p_kw']
    sw_kvar = g_results.loc[swbus, 'q_kvar']
    ex_kw = e_results.loc[exgridbus, 'p_kw']
    ex_kvar = e_results.loc[exgridbus, 'q_kvar']
    g_results.loc[swbus, 'p_kw'] = sw_kw + ex_kw
    g_results.loc[swbus, 'q_kvar'] = sw_kvar + ex_kvar
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
    # -- CALCULATE TOTAL POWER OF PARTICIPATING GENERATORS --------------------
    pgenerators = sum([x for x in g_results['mw'].values if x != 0.0])
    # -- CONVERT PANDAS DATAFRAME TO LIST FOR REPORTING -----------------------
    glist = [g_results.columns.values.tolist()] + g_results.values.tolist()
    # -- WRITE THE GENERATION RESULTS TO FILE ---------------------------------
    write_csvdata(fname, glist, [['--generator section']])
    return g_results, pgenerators


def write_bus_results(fname, b_results, f_dict, s_dict, g_results, sh_results, clabel, exgridbus):
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
    b_results['sw_mvars'] = 0.0
    # -- RENAME COLUMN HEADINGS -----------------------------------------------
    b_results.rename(columns={'vm_pu': 'voltage', 'va_degree': 'angle'}, inplace=True)
    # -- PREVENT NEGATIVE ZEROS -----------------------------------------------
    b_results['voltage'] += 0.0
    b_results['angle'] += 0.0
    # -- CONVERT PANDAS DATAFRAME TO LIST FOR REPORTING -----------------------
    buslist = [b_results.columns.values.tolist()] + b_results.values.tolist()
    # -- GET ANY SHUNT MVARS FOR REPORTING ------------------------------------
    # -- (SWITCHED SHUNTS ARE MODELED AS GENERATORS) --------------------------
    for j in range(1, len(buslist)):
        buslist[j][0] = int(buslist[j][0])
        bus = buslist[j][0]
        mvars = 0.0
        if bus in f_dict:
            mvars = -1e-3 * sh_results.loc[f_dict[bus], 'q_kvar']
        if bus in s_dict:
            mvars = -1e-3 * g_results.loc[s_dict[bus], 'q_kvar']
        if bus in f_dict and bus in s_dict:
            mvars1 = -1e-3 * sh_results.loc[f_dict[bus], 'q_kvar']
            mvars2 = -1e-3 * g_results.loc[s_dict[bus], 'q_kvar']
            mvars = mvars1 + mvars2
        buslist[j][3] = mvars + 0.0
    # -- WRITE THE BUS RESULTS TO FILE ----------------------------------------
    write_csvdata(fname, [], [['--contingency'], ['label'], [clabel]])
    write_csvdata(fname, buslist, [['--bus section']])
    return b_results


def write_gen_results(fname, g_results, genids, gbuses, b_pgens, e_results, swbus, exgridbus):
    g_results.drop(g_results.index[sw_indexes], inplace=True)
    del g_results['vm_pu']
    del g_results['va_degree']
    # -- COMBINE SWING GENERATOR AND EXTERNAL GRID CONTRIBUTIONS --------------
    sw_kw = g_results.loc[swbus, 'p_kw']
    sw_kvar = g_results.loc[swbus, 'q_kvar']
    ex_kw = e_results.loc[exgridbus, 'p_kw']
    ex_kvar = e_results.loc[exgridbus, 'q_kvar']
    g_results.loc[swbus, 'p_kw'] = sw_kw + ex_kw
    g_results.loc[swbus, 'q_kvar'] = sw_kvar + ex_kvar
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
    # -- CALCULATE TOTAL POWER OF PARTICIPATING GENERATORS --------------------
    c_gens = sum([x for x in g_results['mw'].values if x != 0.0])
    # -- CONVERT PANDAS DATAFRAME TO LIST FOR REPORTING -----------------------
    glist = [g_results.columns.values.tolist()] + g_results.values.tolist()
    # -- WRITE THE GENERATION RESULTS TO FILE ---------------------------------
    write_csvdata(fname, glist, [['--generator section']])
    deltapgens = c_gens - b_pgens
    write_csvdata(fname, [], [['--delta section'], ['delta_p'], [deltapgens]])
    return g_results, c_gens


def format_gendispdata(lol):
    # dispdata = ['I', 'ID', 'PARTICIPATION_FACTOR', 'POWER_DISP_TABLE']
    gddict = {}
    if lol != [[]]:
        for i in range(len(lol)):
            lol[i] = [str(lol[i][0]), str(lol[i][1]), float(lol[i][2]), int(lol[i][3])]
        for i in range(len(lol)):
            gkey = lol[i][0] + '-' + lol[i][1]
            gddict.update({gkey: lol[i][3]})
    return gddict


def format_powerdispdata(lol):
    # power_dispdata = ['TABLE_NUM', 'PMAX', 'PMIN', 'FUEL_COST_CONVERSION_FACTOR', 'COST_CURVE_TYPE', 'STATUS", 'COST_TABLE']
    pdict = {}
    if lol != [[]]:
        for i in range(len(lol)):
            lol[i] = [int(lol[i][0]), float(lol[i][1]), float(lol[i][2]), float(lol[i][3]), int(lol[i][4]), int(lol[i][5]), int(lol[i][6])]
        for i in range(len(lol)):
            # pkey = lol[i][0]
            # pdict.update({pkey: [-1e3 * lol[i][1], -1e3 * lol[i][2], bool(lol[i][5]), lol[i][6]]})
            pdict.update({lol[i][0]: lol[i][6]})

    return pdict


def format_pwlcostdata(lol):
    # pwl_header = ['TABLE_NUM', 'TABLE_ID', 'NUM_PIECES']
    # pwl_data = ['MW', 'COST']
    cdict = {}
    if lol != [[]]:
        for i in range(len(lol)):
            if len(lol[i]) == 3:
                lol[i] = [int(lol[i][0]), str(lol[i][1]), int(lol[i][2])]
                ckey = lol[i][0]
                cdict.update({ckey: []})
            if len(lol[i]) == 2:
                lol[i] = [round(-1e3 * float(lol[i][0]), 5), round(float(lol[i][1]), 5)]
                cdict[ckey].append(lol[i])
    for ckey in cdict:
        cdict[ckey].sort()
    return cdict


def print_dataframes_results(_net):
    pdoptions.display.max_columns = 1000
    pdoptions.display.max_rows = 1000
    pdoptions.display.max_colwidth = 199
    pdoptions.display.width = None
    pdoptions.display.precision = 4
    print()
    print('BUS DATAFRAME')
    print(_net.bus)
    print()
    print('BUS RESULTS')
    print(_net.res_bus)
    print()
    print('LINE DATAFRAME')
    print(_net.line)
    print()
    print('LINE RESULTS')
    print(_net.res_line)
    print()
    print('TRANSFORMER DATAFRAME')
    print(_net.trafo)
    print()
    print('TRANSFORMER RESULTS')
    print(_net.res_trafo)
    print()
    print('GENERATOR DATAFRAME')
    print(_net.gen)
    print()
    print('GENERATOR RESULTS')
    print(_net.res_gen)
    print()
    return


# =============================================================================
# -- MAIN ---------------------------------------------------------------------
# =============================================================================
if __name__ == "__main__":
    cwd = os.getcwd()
    start_time = time.time()

    # =========================================================================
    # -- PARSE THE RAW FILE ---------------------------------------------------
    # =========================================================================
    print('GETTING RAW DATA FROM FILE .........................................', os.path.split(raw_fname)[1])
    raw_busdata = []
    raw_loaddata = []
    raw_fixshuntdata = []
    raw_gendata = []
    raw_branchdata = []
    raw_xfmrdata = []
    raw_areaidata = []
    raw_dclinedata = []
    raw_vscdata = []
    raw_xfmricdata = []
    raw_mtdclinedata = []
    raw_mslinedata = []
    raw_zonedata = []
    raw_areaxferdata = []
    raw_ownerdata = []
    raw_factsdata = []
    raw_swshuntdata = []
    raw_gnedata = []
    raw_machinedata = []
    raw_rawdata = [raw_busdata, raw_loaddata, raw_fixshuntdata, raw_gendata, raw_branchdata, raw_xfmrdata, raw_areaidata, raw_dclinedata, raw_vscdata, raw_xfmricdata,
                   raw_mtdclinedata, raw_mslinedata, raw_zonedata, raw_areaxferdata, raw_ownerdata, raw_factsdata, raw_swshuntdata, raw_gnedata, raw_machinedata]
    dataobj = get_raw_csvdata(raw_fname)
    line = next(dataobj)
    mva_base = float(line[1])
    basefreq = float(line[5][:4])
    x = next(dataobj)
    line = next(dataobj)
    for record in raw_rawdata:
        if line[0].startswith('Q'):
            break
        while True:
            line = next(dataobj)
            if line[0].startswith('0 ') or line[0].startswith('Q'):
                break
            record.append(line)
    # =========================================================================
    if not raw_busdata: raw_busdata = [[]]
    if not raw_loaddata: raw_loaddata = [[]]
    if not raw_fixshuntdata: raw_fixshuntdata = [[]]
    if not raw_gendata: raw_gendata = [[]]
    if not raw_branchdata: raw_branchdata = [[]]
    if not raw_xfmrdata: raw_xfmrdata = [[]]
    if not raw_zonedata: raw_zonedata = [[]]
    if not raw_ownerdata: raw_ownerdata = [[]]
    if not raw_swshuntdata: raw_swshuntdata = [[]]

    # -- SEPARATE 2WXFMRS AND 3WXFMRS -----------------------------------------
    raw_xfmr2wdata, raw_xfmr3wdata = split_xfmrdata(raw_xfmrdata)
    if not raw_xfmr2wdata: raw_xfmr2wdata = [[]]
    if not raw_xfmr3wdata: raw_xfmr3wdata = [[]]

    # -- ASSIGN DATA TYPES TO RAW DATA ----------------------------------------
    # print('FORMATTING RAW DATA ................................................')
    busdata, areas = format_busdata(raw_busdata)
    loaddata = format_loaddata(raw_loaddata)
    fixshuntdata = format_fixshuntdata(raw_fixshuntdata)
    gendata = format_gendata(raw_gendata)
    branchdata = format_branchdata(raw_branchdata)
    xfmr2wdata = format_xfmr2wdata(raw_xfmr2wdata)
    # xfmr3wdata = format_xfmr3wdata(raw_xfmr3wdata)
    zonedata = format_zonedata(raw_zonedata)
    swshuntdata = format_swshuntdata(raw_swshuntdata)

    # -- GET SWING BUS FROM RAW BUSDATA ---------------------------------------
    swingbus, swing_name, swing_kv, swing_angle, swing_vlow, swing_vhigh = get_swingbus_data(raw_busdata)

    # -- GET SWING GEN REGULATED VOLTAGE FROM GENDATA -------------------------
    swing_key, swing_vreg, swing_qmin, swing_qmax, swing_pmin, swing_pmax = get_swing_gen_data(raw_gendata, swingbus)

    # =========================================================================
    # -- PARSE CON FILE -------------------------------------------------------
    # =========================================================================
    print()
    print('GETTING CONTINGENCY DATA FROM FILE .................................', os.path.split(con_fname)[1])
    outagedict = get_contingencies(con_fname)
    # =========================================================================

    # =========================================================================
    # -- PARSE THE ROP FILE ---------------------------------------------------
    # =========================================================================
    print('GETTING GENERATOR OPF DATA FROM FILE ...............................', os.path.split(rop_fname)[1])
    rop_icode = None
    rop_busvattdata = []
    rop_adjshuntdata = []
    rop_loaddata = []
    rop_adjloaddata = []
    rop_gendispdata = []
    rop_powerdispdata = []
    rop_genresdata = []
    rop_genreactdata = []
    rop_adjbranchreactdata = []
    rop_pwlcostdata = []
    rop_pwqcostdata = []
    rop_polyexpcostdata = []
    rop_periodresdata = []
    rop_branchflowdata = []
    rop_interfaceflowdata = []
    rop_linearconstdata = []
    rop_ropdata = [rop_busvattdata, rop_adjshuntdata, rop_loaddata, rop_adjloaddata, rop_gendispdata, rop_powerdispdata,
                   rop_genresdata, rop_genreactdata, rop_adjbranchreactdata, rop_pwlcostdata, rop_pwqcostdata, rop_polyexpcostdata,
                   rop_periodresdata, rop_branchflowdata, rop_interfaceflowdata, rop_linearconstdata]
    dataobj = get_raw_csvdata(rop_fname)
    line = []
    while not line:
        line = next(dataobj)
    rop_icode = line[0][:1]
    for record in rop_ropdata:
        line = next(dataobj)
        if line[0].startswith('0 '):
            continue
        while True:
            if line[0].startswith('0 '):
                break
            record.append(line)
            line = next(dataobj)
    # =========================================================================
    if not rop_gendispdata: rop_gendispdata = [[]]
    if not rop_powerdispdata: rop_powerdispdata = [[]]
    if not rop_pwlcostdata: rop_pwlcostdata = [[]]

    # -- ASSIGN DATA TYPES TO ROP DATA AND CONVERT TO DICTS -------------------
    # print('FORMATTING ROP DATA ................................................')
    genopfdict = format_gendispdata(rop_gendispdata)
    gdispdict = format_powerdispdata(rop_powerdispdata)
    pwlcostdata = format_pwlcostdata(rop_pwlcostdata)

    # =========================================================================
    # -- PARSE THE INL FILE ---------------------------------------------------
    # =========================================================================
    print('GETTING GENERATOR RESERVES FROM FILE ...............................', os.path.split(inl_fname)[1])
    greservedata = get_gen_reserves(inl_fname)
    # =========================================================================

    # print('PARTICIPATING GENERATORS MAPPED TO PMAX-PMIN-COSTCURVE TABLES')
    # for j in genopfdict:
    #     print(j, genopfdict[j])
    # print('PMAX-PMIN-COSTCURVE TABLES')
    # for j in gdispdict:
    #     print(j, gdispdict[j])
    # print('PWL COST-CURVE TABLES')
    # for j in pwlcostdata:
    #     print(j, pwlcostdata[j])

    # == CREATE NETWORK =======================================================
    print('------------------------- CREATING NETWORK -------------------------')
    kva_base = 1000 * mva_base
    net = pp.create_empty_network('net', basefreq, kva_base)

    # == ADD BUSES TO NETWORK =================================================
    # bus = ['I', 'NAME', 'BASKV', 'IDE', 'AREA', 'ZONE', 'OWNER', 'VM', 'VA', 'NVHI', 'NVLO', 'EVHI', 'EVLO']
    busdict = {}
    if busdata != [[]]:
        print('ADD BUSES ..........................................................')
        for data in busdata:
            busnum = data[0]
            busname = data[1]
            buskv = data[2]
            buszone = data[5]
            status = abs(data[3]) < 4
            vmax = data[11]
            vmin = data[12]
            busdict.update({busnum: buskv})
            idx = pp.create_bus(net, vn_kv=buskv, name=busname, index=busnum, type="b", zone=buszone, in_service=status, max_vm_pu=vmax, min_vm_pu=vmin)

    # == ADD LOADS TO NETWORK =================================================
    print('ADD LOADS ..........................................................')
    # load = ['I', 'ID', 'STATUS', 'AREA', 'ZONE', 'PL', 'QL', 'IP', 'IQ', 'YP', 'YQ', 'OWNER', 'SCALE', 'INTRPT']
    if loaddata != [[]]:
        for data in loaddata:
            loadbus = data[0]
            loadname = data[1]
            status = bool(data[2])
            loadp = data[5] * 1e3
            loadq = data[6] * 1e3
            pp.create_load(net, loadbus, loadp, q_kvar=loadq, name=loadname, scaling=1.0, index=loadbus, in_service=status,
                           max_p_kw=loadp, min_p_kw=loadp, max_q_kvar=loadq, min_q_kvar=loadq, controllable=False)

    # == ADD GENERATORS TO NETWORK ============================================
    # gens = ['I', 'ID', 'PG', 'QG', 'QT', 'QB', 'VS', 'IREG', 'MBASE', 'ZR', 'ZX', 'RT', 'XT', 'GTAP', 'STAT', 'RMPCT', 'PT', 'PB',
    #         'O1', 'F1', 'O2', 'F2', 'O3', 'F3', 'O4', 'F4', 'WMOD', 'WPF']
    genbuses = []
    gids = []
    genreg = {}
    gendict = {}
    greservedict = {}
    if gendata != [[]]:
        print('ADD GENERATORS .....................................................')
        for data in gendata:
            genbus = data[0]
            gid = data[1]
            pgen = data[2]
            qgen = data[3]
            qmin = data[4]
            qmax = data[5]
            vreg = data[6]
            status = bool(data[14])
            pmin = data[16]
            pmax = data[17]
            pcostdata = None
            genkey = str(genbus) + '-' + gid
            if genkey in genopfdict:
                disptablekey = genopfdict[genkey]
                costtablekey = gdispdict[disptablekey]
                pcostdata = numpy.array(pwlcostdata[costtablekey])
                if genkey in greservedata:
                    greserve = greservedata[genkey]

                    idx = pp.create_gen(net, genbus, pgen, vm_pu=vreg, name=gid, min_p_kw=pmin, max_p_kw=pmax,
                                        scaling=1.0, controllable=False, in_service=status, index=genbus)
                    pp.create_piecewise_linear_cost(net, idx, 'gen', pcostdata, type='p')

                    gendict.update({genkey: idx})
                    genreg.update({genbus: vreg})
                    greservedict.update({genbus: greserve})
                    genbuses.append(genbus)
                    gids.append("'" + gid + "'")
            else:
                idx = pp.create_gen(net, genbus, pgen, vm_pu=vreg, name=gid,
                                    scaling=1.0, controllable=False, in_service=status, index=genbus)
                gendict.update({genkey: idx})
                genreg.update({genbus: vreg})
                greservedict.update({genbus: pmin})
                genbuses.append(genbus)
                gids.append("'" + gid + "'")

            # TODO --- save greserve in dict for contingency analysis... pmin = pbase - greserve

    # == ADD FIXED SHUNT DATA TO NETWORK ======================================
    # fixshunt = ['I', 'ID', 'STATUS', 'GL', 'BL']
    fx_dict = {}
    if fixshuntdata != [[]]:
        print('ADD FIXED SHUNTS ...................................................')
        for data in fixshuntdata:
            shuntbus = data[0]
            shuntname = str(shuntbus) + 'fx'
            status = data[2]
            kvar = -1e3 * data[4]
            idx = pp.create_shunt(net, shuntbus, kvar, step=1, max_step=True, name=shuntname, in_service=True, index=shuntbus)
            # idx = pp.create_shunt(net, shuntbus, kvar, index=shuntbus)
            fx_dict.update({shuntbus: idx})

    # == ADD SWITCHED SHUNTS TO NETWORK =======================================
    # TODO ----- DONE ----- SWITCHED SHUNTS AS DYNAMIC REACTIVE GENERATORS ----
    # swshunt = ['I', 'MODSW', 'ADJM', 'STAT', 'VSWHI', 'VSWLO', 'SWREM', 'RMPCT', 'RMIDNT', 'BINIT', 'N1', 'B1',
    #            'N2', 'B2', 'N3', 'B3', 'N4', 'B4', 'N5', 'B5', 'N6', 'B6', 'N7', 'B7', 'N8', 'B8']
    sw_dict = {}
    sw_indexes = []
    if swshuntdata != [[]]:
        print('ADD SWITCHED SHUNTS ................................................')
        for data in swshuntdata:
            shuntbus = data[0]
            status = data[3]
            max_vreg = data[4]
            min_vreg = data[5]
            vreg = round((max_vreg + min_vreg) / 2.0, 4)
            if shuntbus in genreg:
                vreg = genreg[shuntbus]
            remote_bus = data[6]
            kvar_int = -1e3 * data[9]
            steps = [data[10], data[12], data[14], data[16], data[18], data[20], data[22], data[24]]
            kvars = -1000 * [data[11], data[13], data[15], data[17], data[19], data[21], data[23], data[25]]
            total_inductance = 0.0
            total_capacitance = 0.0
            for j in range(len(kvars)):
                if kvars[j] < 0.0:
                    total_capacitance += steps[j] * kvars[j]
                elif kvars[j] > 0.0:
                    total_inductance += steps[j] * kvars[j]
            # NO SWSHUNTS IN SANDBOX
            print('SWITCHED SHUNT REACTIVE =', total_inductance, total_capacitance)
            idx = pp.create_gen(net, shuntbus, 0.0, vm_pu=vreg, max_q_kvar=total_capacitance, min_q_kvar=total_inductance,
                                min_p_kw=0.0, max_p_kw=0.0, controllable=False, in_service=status)
            sw_dict.update({shuntbus: idx})
            sw_indexes.append(idx)

    # == ADD LINES TO NETWORK =================================================
    # branch = ['I', 'J', 'CKT', 'R', 'X', 'B', 'RATEA', 'RATEB', 'RATEC', 'GI', 'BI', 'GJ', 'BJ', 'ST', 'MET', 'LEN',
    #           'O1', 'F1', 'O2', 'F2', 'O3', 'F3', 'O4', 'F4']
    linedict = {}
    if branchdata != [[]]:
        print('ADD LINES ..........................................................')
        for data in branchdata:
            frombus = data[0]
            tobus = data[1]
            ckt = data[2]
            status = bool(data[13])
            length = data[15]
            if length == 0.0:
                length = 1.0
            kv = busdict[frombus]
            zbase = kv ** 2 / mva_base
            r_pu = data[3] / length
            x_pu = data[4] / length
            b_pu = data[5] / length
            r = r_pu * zbase
            x = x_pu * zbase
            b = b_pu / zbase
            capacitance = 1e9 * b / (2 * math.pi * basefreq)
            if CONRATING == 0:
                mva_rating = data[6]
            elif CONRATING == 1:
                mva_rating = data[7]
            elif CONRATING == 2:
                mva_rating = data[8]
            i_rating = mva_rating / (math.sqrt(3) * kv)
            line_index = int(str(frombus) + str(tobus))
            linekey = str(frombus) + '-' + str(tobus) + '-' + ckt
            idx = pp.create_line_from_parameters(net, frombus, tobus, length, r, x, capacitance, i_rating, name=linekey,
                                                 in_service=status, df=1.0, parallel=1, max_loading_percent=100.0, index=line_index)
            linedict.update({linekey: idx})

    # == ADD 2W TRANSFORMERS TO NETWORK =======================================
    # 2wxfmr = ['I', 'J', 'K', 'CKT', 'CW', 'CZ', 'CM', 'MAG1', 'MAG2', 'NMETR', 'NAME', 'STAT', 'O1', 'F1', 'O2', 'F2', 'O3', 'F3', 'O4', 'F4', 'VECGRP',
    #           'R1-2', 'X1-2', 'SBASE1-2', 'WINDV1', 'NOMV1', 'ANG1', 'RATA1', 'RATB1', 'RATC1', 'COD1', 'CONT1', 'RMA1', 'RMI1', 'VMA1', 'VMI1',
    #           'NTP1', 'TAB1', 'CR1', 'CX1', 'CNXA1', 'WINDV2', 'NOMV2']
    xfmrdict = {}
    xfmr_ratea_dict = {}
    if xfmr2wdata != [[]]:
        print('ADD 2W TRANSFORMERS ................................................')
        for data in xfmr2wdata:
            status = bool(data[11])
            frombus = data[0]
            tobus = data[1]
            ckt = data[3]
            fromkv = busdict[frombus]
            tokv = busdict[tobus]
            tap1 = data[24]
            tap2 = data[41]
            if fromkv < tokv:                           # force from bus to be highside
                frombus, tobus = tobus, frombus
                fromkv, tokv = tokv, fromkv
                tap1, tap2 = tap2, tap1
            net_tap = tap1 / tap2                       # net tap setting on highside
            phaseshift = data[26]
            r_pu = data[21]                             # @ mva_base
            x_pu = data[22]                             # @ mva_base
            mva_rating_a = data[27]
            mva_rating_b = data[28]
            mva_rating_c = data[28]
            if CONRATING == 0:                          # if global is rate a
                mva_rating = mva_rating_b
            elif CONRATING == 1:                        # if global is rate b
                mva_rating = mva_rating_c
            elif CONRATING == 2:                        # if global is rate c
                mva_rating = data[29]
            r_pu *= mva_rating / mva_base               # pandapower uses given transformer rating as test mva
            x_pu *= mva_rating / mva_base               # so convert to mva_rating base
            z_pu = math.sqrt(r_pu ** 2 + x_pu ** 2)     # calculate 'nameplate' pu impedance
            z_pct = 100.0 * z_pu                        # pandadower uses percent impedance
            r_pct = 100.0 * r_pu                        # pandadower uses percent resistance
            kva_rating = 1e3 * mva_rating               # pandadower uses kva instead of mva
            kva_rating_a = 1e3 * mva_rating_a           # capture rate a for base case analysis
            tapside = 'hv'                              # use highside tap setting
            tap_pct = 100.0 * abs(1 - net_tap)          # calculate off-nominal percent
            tapmax = 2
            tapmid = 0
            tapmin = -2
            if net_tap > 1.0:
                tappos = 1
            elif net_tap == 1.0:
                tappos = 0
            elif net_tap < 1.0:
                tappos = -1
            noloadlosses = 100.0 * data[7]              # % no-load current / full-load current
            ironlosses = 0.0
            xfmr2wkey = str(frombus) + '-' + str(tobus) + '-' + ckt
            xfmr2windex = int(str(frombus) + str(tobus))

            idx = pp.create_transformer_from_parameters(net, frombus, tobus, kva_rating, fromkv, tokv, r_pct, z_pct, ironlosses, noloadlosses, shift_degree=phaseshift,
                                                        tp_side=tapside, tp_mid=tapmid, tp_max=tapmax, tp_min=tapmin, tp_st_percent=tap_pct, tp_pos=tappos, in_service=status,
                                                        max_loading_percent=100.0, parallel=1, df=1.0, index=xfmr2windex, name=xfmr2wkey)
            xfmrdict.update({xfmr2wkey: idx})
            xfmr_ratea_dict.update({xfmr2wkey: kva_rating_a})

    # == ADD EXTERNAL GRID (PARALLEL TO SWING BUS) ============================
    ext_grid_bus = pp.create_bus(net, vn_kv=swing_kv, name='Ex_Grid_Bus', in_service=True, max_vm_pu=swing_vhigh, min_vm_pu=swing_vlow)
    ext_tie_rating = 1e6/(math.sqrt(3) * swing_kv)
    tie_index = int(str(swingbus) + str(ext_grid_bus))
    pp.create_line_from_parameters(net,  swingbus, ext_grid_bus, 1.0, 0.0, 0.002, 0.0, ext_tie_rating, name='Swing-Tie', in_service=True, df=1.0, index=tie_index)
    pp.create_ext_grid(net, ext_grid_bus, vm_pu=swing_vreg, va_degree=swing_angle, in_service=True, min_p_kw=-1, max_p_kw=1,
                       min_q_kvar=-1, max_q_kvar=1, index=ext_grid_bus)
    # pp.create_ext_grid(net, ext_grid_bus, vm_pu=swing_vreg, va_degree=swing_angle, in_service=True, min_p_kw=-1e9, max_p_kw=1e9, min_q_kvar=-1e9, max_q_kvar=1e9,
    #                    index=ext_grid_bus)
    # pp.create_ext_grid(net, ext_grid_bus, vm_pu=swing_vreg, va_degree=swing_angle, in_service=True, min_p_kw=swing_pmin, max_p_kw=swing_pmax,
    #                    min_q_kvar=swing_qmin, max_q_kvar=swing_qmax, index=ext_grid_bus)
    # pp.create_ext_grid(net, ext_grid_bus, vm_pu=swing_vreg, va_degree=swing_angle, in_service=True, index=ext_grid_bus)

    # pp.create_polynomial_cost(net, ext_grid_bus, 'ext_grid', numpy.array([-0.0008, 0]), type='p')
    # pp.create_polynomial_cost(net, ext_grid_bus, 'ext_grid', numpy.array([-0.0008, 0]), type='q')
    # pp.create_polynomial_cost(net, ext_grid_bus, 'ext_grid', numpy.array([-1, 0]), type='p')
    print('--------------------------------------------------------------------')
    # -- DONE CREATING NETWORK ------------------------------------------------

    # -- DIAGNOSTIC DEVELOPMENT -----------------------------------------------
    # pp.diagnostic(net, report_style='detailed', warnings_only=False)

    try:
        os.remove(outfname1)
    except FileNotFoundError:
        pass
    try:
        os.remove(outfname2)
    except FileNotFoundError:
        pass

    # RUN_OPF = 0
    # =========================================================================
    # -- PROCESS BASECASE POWER FLOW ------------------------------------------
    # =========================================================================
    if not RUN_OPF:
        # -- SOLVE BASECASE POWER FLOW ----------------------------------------
        print('SOLVING BASECASE POWER FLOW ........................................')
        pp.runpp(net, init='auto', max_iteration=ITMXN, calculate_voltage_angles=True, enforce_q_lims=True)
        base_net = copy.deepcopy(net)

        # -- GET BASECASE RESULTS FROM SOLUTION -------------------------------
        base_shunt_results = net.res_shunt
        base_gen_results = net.res_gen
        base_bus_results = net.res_bus
        base_extgrid_results = net.res_ext_grid

        # print_dataframes_results(net)

        # -- WRITE BASECASE BUS AND GENERATOR RESULTS TO FILE -----------------
        base_bus_results = write_base_bus_results(outfname1, base_bus_results, fx_dict, sw_dict, base_gen_results, base_shunt_results, ext_grid_bus)
        base_gen_results, base_pgens = write_base_gen_results(outfname1, base_gen_results, gids, genbuses, base_extgrid_results, swingbus, ext_grid_bus)
        print('DONE WITH BASECASE POWER FLOW.......................................', round(time.time() - start_time, 1))
        print()

        # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
        # -- PROCESS POWER FLOW CONTINGENCIES  --------------------------------
        # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
        start_time = time.time()
        if outagedict['gen']:
            print('RUNNING GENERATOR OUTAGES ..........................................')
        for j in outagedict['gen']:
            net = copy.deepcopy(base_net)
            conlabel = outagedict['gen'][j]
            if conlabel in gendict:
                genidx = gendict[conlabel]
                net.gen.in_service[genidx] = False
            else:
                print('GENERATOR NOT FOUND ................................................', conlabel)
            pp.runpp(net, init='auto', max_iteration=ITMXN, calculate_voltage_angles=True, enforce_q_lims=True)

            # -- CONTINGENCY RESULTS FROM SOLUTION ----------------------------
            bus_results = net.res_bus
            shunt_results = net.res_shunt
            gen_results = net.res_gen
            extgrid_results = net.res_ext_grid

            # -- WRITE CONTINGENCY BUS AND GENERATOR RESULTS TO FILE ----------
            conlabel = "'" + outagedict['gen'][j] + "'"
            bus_results = write_bus_results(outfname2, bus_results, fx_dict, sw_dict, gen_results, shunt_results, conlabel, ext_grid_bus)
            gen_results, pgens = write_gen_results(outfname2, gen_results, gids, genbuses, base_pgens, extgrid_results, swingbus, ext_grid_bus)

        if outagedict['branch']:
            print('RUNNING LINE AND TRANSFORMER OUTAGES ...............................')
        for j in outagedict['branch']:
            net = copy.deepcopy(base_net)
            conlabel = outagedict['branch'][j]
            if conlabel in linedict:
                lineidx = linedict[conlabel]
                net.line.in_service[lineidx] = False
            elif conlabel in xfmrdict:
                xfmridx = xfmrdict[conlabel]
                net.trafo.in_service[xfmridx] = False
            else:
                print('LINE OR TRANSFORMER NOT FOUND ......................................', conlabel)
            pp.runpp(net, init='auto', max_iteration=ITMXN, calculate_voltage_angles=True)

            # -- CONTINGENCY RESULTS FROM SOLUTION ----------------------------
            bus_results = net.res_bus
            shunt_results = net.res_shunt
            gen_results = net.res_gen
            extgrid_results = net.res_ext_grid

            # -- WRITE CONTINGENCY BUS AND GENERATOR RESULTS TO FILE ----------
            conlabel = "'" + outagedict['branch'][j] + "'"
            bus_results = write_bus_results(outfname2, bus_results, fx_dict, sw_dict, gen_results, shunt_results, conlabel, ext_grid_bus)
            gen_results, pgens = write_gen_results(outfname2, gen_results, gids, genbuses, base_pgens, extgrid_results, swingbus, ext_grid_bus)

        print('DONE WITH CONTINGENCIES POWER FLOW .................................', round(time.time() - start_time, 1))

    # =========================================================================
    # -- PROCESS BASECASE OPTIMAL POWER FLOW ----------------------------------
    # =========================================================================
    if RUN_OPF:
        # -- SOLVE BASECASE OPTIMAL POWER FLOW --------------------------------
        print('SOLVING BASECASE OPTIMAL POWER FLOW ................................')
        pp.runopp(net,  init='flat', calculate_voltage_angles=True, verbose=True, suppress_warnings=True)
        opf_base_net = copy.deepcopy(net)

        # -- GET BASECASE OPF RESULTS FROM SOLUTION ---------------------------
        base_shunt_results = net.res_shunt
        base_gen_results = net.res_gen
        base_bus_results = net.res_bus
        base_extgrid_results = net.res_ext_grid

        # -- DEVELOPEMENT -----------------------------------------------------
        # print_dataframes_results(net)

        # -- WRITE BASECASE BUS AND GENERATOR RESULTS TO FILE -----------------
        base_bus_results = write_base_bus_results(outfname1, base_bus_results, fx_dict, sw_dict, base_gen_results, base_shunt_results, ext_grid_bus)
        base_gen_results, base_pgens = write_base_gen_results(outfname1, base_gen_results, gids, genbuses, base_extgrid_results, swingbus, ext_grid_bus)
        print('DONE WITH BASECASE OPTIMAL POWER FLOW...............................', round(time.time() - start_time, 1))
        print()

        # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
        # -- PROCESS OPF CONTINGENCIES ----------------------------------------
        # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
        # print(greservedict)

        start_time = time.time()
        if outagedict['gen']:
            print('RUNNING OPF GENERATOR OUTAGES ......................................')
        for j in outagedict['gen']:
            net = copy.deepcopy(opf_base_net)
            conlabel = outagedict['gen'][j]
            if conlabel in gendict:
                genidx = gendict[conlabel]
                net.gen.in_service[genidx] = False
            else:
                print('GENERATOR NOT FOUND ................................................', conlabel)
            pp.runopp(net, init='flat', calculate_voltage_angles=True, verbose=True, suppress_warnings=True)

            # -- CONTINGENCY RESULTS FROM SOLUTION ----------------------------
            bus_results = net.res_bus
            shunt_results = net.res_shunt
            gen_results = net.res_gen
            extgrid_results = net.res_ext_grid

            # -- WRITE CONTINGENCY BUS AND GENERATOR RESULTS TO FILE ----------
            conlabel = "'" + outagedict['gen'][j] + "'"
            bus_results = write_bus_results(outfname2, bus_results, fx_dict, sw_dict, gen_results, shunt_results, conlabel, ext_grid_bus)
            gen_results, pgens = write_gen_results(outfname2, gen_results, gids, genbuses, base_pgens, extgrid_results, swingbus, ext_grid_bus)

        if outagedict['branch']:
            print('RUNNING OPF LINE AND TRANSFORMER OUTAGES ...........................')
        for j in outagedict['branch']:
            net = copy.deepcopy(opf_base_net)
            conlabel = outagedict['branch'][j]
            if conlabel in linedict:
                lineidx = linedict[conlabel]
                net.line.in_service[lineidx] = False
            elif conlabel in xfmrdict:
                xfmridx = xfmrdict[conlabel]
                net.trafo.in_service[xfmridx] = False
            else:
                print('LINE OR TRANSFORMER NOT FOUND ......................................', conlabel)
            pp.runopp(net, init='flat', calculate_voltage_angles=True, verbose=True, suppress_warnings=True)

            # -- CONTINGENCY RESULTS FROM SOLUTION ----------------------------
            bus_results = net.res_bus
            shunt_results = net.res_shunt
            gen_results = net.res_gen
            extgrid_results = net.res_ext_grid

            # -- WRITE CONTINGENCY BUS AND GENERATOR RESULTS TO FILE ----------
            conlabel = "'" + outagedict['branch'][j] + "'"
            bus_results = write_bus_results(outfname2, bus_results, fx_dict, sw_dict, gen_results, shunt_results, conlabel, ext_grid_bus)
            gen_results, pgens = write_gen_results(outfname2, gen_results, gids, genbuses, base_pgens, extgrid_results, swingbus, ext_grid_bus)

        print('DONE WITH OPF CONTINGENCIES ........................................', round(time.time() - start_time, 1))
        # print()
        # print('GENERATOR RESERVES')
        # print(greservedict)
