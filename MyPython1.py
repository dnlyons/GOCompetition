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

# -- DEVELOPMENT DEFAULT ------------------------------------------------------
if not sys.argv[1:]:
    con_fname = cwd + r'/sandbox/scenario_1/case0.con'
    inl_fname = cwd + r'/sandbox/scenario_1/case0.inl'
    raw_fname = cwd + r'/sandbox/scenario_1/case0.raw'
    rop_fname = cwd + r'/sandbox/scenario_1/case0.rop'
    outfname1 = cwd + r'/sandbox/scenario_1/solution1.txt'
    outfname2 = cwd + r'/sandbox/scenario_1/solution2.txt'

# -- USING COMMAND LINE -------------------------------------------------------
if sys.argv[1:]:
    print()
    con_fname = sys.argv[1]
    inl_fname = sys.argv[2]
    raw_fname = sys.argv[3]
    rop_fname = sys.argv[4]
    outfname1 = 'solution1.txt'
    outfname2 = 'solution2.txt'

BASERATING = 2       # contingency line and xfmr ratings 0=RateA, 1=RateB, 2=RateC
CONRATING = 2       # contingency line and xfmr ratings 0=RateA, 1=RateB, 2=RateC
ITMXN = 40          # max iterations solve option


# =============================================================================
# -- FUNCTIONS ----------------------------------------------------------------
# =============================================================================
def listoflists(tt):
    """ convert tuple_of_tuples to list_of_lists """
    return list((listoflists(x) if isinstance(x, tuple) else x for x in tt))


def tupleoftuples(ll):
    """ convert list_of_lists to tuple_of_tuples """
    return tuple((tupleoftuples(x) if isinstance(x, list) else x for x in ll))


def get_csvdata(fname):
    with open(fname, 'r') as fobject:
        reader = csv.reader(fobject, delimiter=',', quotechar="'")
        for row in reader:
            row = [x.strip() for x in row]
            yield row
    fobject.close()
    return


def get_raw_data(fname):
    r_busdata = []
    r_loaddata = []
    r_fixshuntdata = []
    r_gendata = []
    r_branchdata = []
    r_xfmrdata = []
    r_areaidata = []
    r_dclinedata = []
    r_vscdata = []
    r_xfmricdata = []
    r_mtdclinedata = []
    r_mslinedata = []
    r_zonedata = []
    r_areaxferdata = []
    r_ownerdata = []
    r_factsdata = []
    r_swshuntdata = []
    r_gnedata = []
    r_machinedata = []
    r_rawdata = [r_busdata, r_loaddata, r_fixshuntdata, r_gendata, r_branchdata, r_xfmrdata, r_areaidata, r_dclinedata, r_vscdata, r_xfmricdata,
                   r_mtdclinedata, r_mslinedata, r_zonedata, r_areaxferdata, r_ownerdata, r_factsdata, r_swshuntdata, r_gnedata, r_machinedata]
    dataobj = get_csvdata(fname)
    line = next(dataobj)
    sbase = float(line[1])
    freq = float(line[5][:4])
    line = next(dataobj)
    line = next(dataobj)
    for record in r_rawdata:
        if line[0].startswith('Q'):
            break
        while True:
            line = next(dataobj)
            if line[0].startswith('0 ') or line[0].startswith('Q'):
                break
            record.append(line)
    return sbase, freq, r_busdata, r_loaddata, r_fixshuntdata, r_gendata, r_branchdata, r_xfmrdata, r_swshuntdata


def get_rop_data(fname):
    r_icode = None
    r_busvattdata = []
    r_adjshuntdata = []
    r_loaddata = []
    r_adjloaddata = []
    r_gendispdata = []
    r_powerdispdata = []
    r_genresdata = []
    r_genreactdata = []
    r_adjbranchreactdata = []
    r_pwlcostdata = []
    r_pwqcostdata = []
    r_polyexpcostdata = []
    r_periodresdata = []
    r_branchflowdata = []
    r_interfaceflowdata = []
    r_linearconstdata = []
    r_ropdata = [r_busvattdata, r_adjshuntdata, r_loaddata, r_adjloaddata, r_gendispdata, r_powerdispdata,
                   r_genresdata, r_genreactdata, r_adjbranchreactdata, r_pwlcostdata, r_pwqcostdata, r_polyexpcostdata,
                   r_periodresdata, r_branchflowdata, r_interfaceflowdata, r_linearconstdata]
    dataobj = get_csvdata(fname)
    line = []
    while not line:
        line = next(dataobj)
    r_icode = line[0][:1]
    for record in r_ropdata:
        line = next(dataobj)
        if line[0].startswith('0 '):
            continue
        while True:
            if line[0].startswith('0 '):
                break
            record.append(line)
            line = next(dataobj)
    return r_gendispdata, r_powerdispdata, r_pwlcostdata

def get_con_csvdata(fname):
    with open(fname, 'r') as fobject:
        reader = csv.reader(fobject, delimiter=' ', quotechar="'", skipinitialspace=True)
        for row in reader:
            row = [x.strip() for x in row]
            yield row
    fobject.close()
    return


def get_reserve_csvdata(fname):
    with open(fname, 'r') as fobject:
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
                    condict['branch'].update({bkey: clabel})
                if len(line) == 6:
                    gkey = line[5] + '-' + line[2]
                    condict['gen'].update({gkey: clabel})
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
        pfdict.update({gkey: float(line[5])})
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
            lol[i] = [int(lol[i][0]), str(lol[i][1]), int(lol[i][2]), -1e3 * float(lol[i][3]), -1e3 * float(lol[i][4])]
    return lol


def format_gendata(lol):
    # gens = ['I', 'ID', 'PG', 'QG', 'QT', 'QB', 'VS', 'IREG', 'MBASE', 'ZR', 'ZX', 'RT', 'XT', 'GTAP', 'STAT', 'RMPCT', 'PT', 'PB',
    #         'O1', 'F1', 'O2', 'F2', 'O3', 'F3', 'O4', 'F4', 'WMOD', 'WPF']
    if lol != [[]]:
        for i in range(len(lol)):
            lol[i] = [int(lol[i][0]), str(lol[i][1]),
                      -1e3 * float(lol[i][2]), -1e3 * float(lol[i][3]),
                      -1e3 * float(lol[i][4]), -1e3 * float(lol[i][5]),
                      float(lol[i][6]), int(lol[i][7]), float(lol[i][8]), float(lol[i][9]), float(lol[i][10]),
                      float(lol[i][11]), float(lol[i][12]), float(lol[i][13]), int(lol[i][14]), float(lol[i][15]),
                      -1e3 * float(lol[i][16]),
                      -1e3 * float(lol[i][17]),
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


# def format_zonedata(lol):
#     if lol != [[]]:
#         for i in range(len(lol)):
#             lol[i] = [int(lol[i][0]), str(lol[i][1])]
#     return lol


def format_ownerdata(lol):
    if lol != [[]]:
        for i in range(len(lol)):
            lol[i] = [int(lol[i][0]), str(lol[i][1])]
    return lol


def format_swshuntdata(lol):
    # I, MODSW, ADJM, STAT, VSWHI, VSWLO, SWREM, RMPCT, RMIDNT, BINIT, N1, B1, N2, B2, N3, B3, N4, B4, N5, B5, N6, B6, N7, B7, N8, B8
    if lol != [[]]:
        for i in range(len(lol)):
            lol[i] = [

                int(lol[i][0]), int(lol[i][1]), int(lol[i][2]), int(lol[i][3]), float(lol[i][4]),
                float(lol[i][5]), int(lol[i][6]), float(lol[i][7]), str(lol[i][8]), -1e3 * float(lol[i][9]),
                int(lol[i][10]), -1e3 * float(lol[i][11]), int(lol[i][12]), -1e3 * float(lol[i][13]), int(lol[i][14]), -1e3 * float(lol[i][15]),
                int(lol[i][16]), -1e3 * float(lol[i][17]), int(lol[i][18]), -1e3 * float(lol[i][19]), int(lol[i][20]),  -1e3 * float(lol[i][21]),
                int(lol[i][22]), -1e3 * float(lol[i][23]), int(lol[i][24]), -1e3 * float(lol[i][25])]
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
    # gens = ['I', 'ID', '-PG', '-QG', 'QT', 'QB', 'VS', 'IREG', 'MBASE', 'ZR', 'ZX', 'RT', 'XT', 'GTAP', 'STAT', 'RMPCT', '-PT', '-PB',
    #         'O1', 'F1', 'O2', 'F2', 'O3', 'F3', 'O4', 'F4', 'WMOD', 'WPF']
    swgens_data = []
    for i in lol:
        if i[0] == swbus:
            sw_key = str(i[0]) + '-' + i[1]
            sw_id = i[1]
            sw_pgen = i[2]
            sw_qgen = i[3]
            sw_qmin = i[4]
            sw_qmax = i[5]
            vreg_sw = i[6]
            sw_status = i[14]
            sw_pmin = i[16]
            sw_pmax = i[17]
            swgens_data.append([sw_key, sw_id, vreg_sw,  sw_pgen, sw_pmin, sw_pmax, sw_qgen, sw_qmin, sw_qmax, sw_status])
            # break
    gdata = [x for x in lol if x[0] != swbus]
    return gdata, swgens_data


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
        if bus in sw_dict:
            mvars = -1e-3 * g_results.loc[sw_dict[bus], 'q_kvar']
            buslist[j][3] = mvars + 0.0
    # -- WRITE THE BUS RESULTS TO FILE ----------------------------------------
    write_csvdata(fname, buslist, [['--bus section']])
    return


def write_base_gen_results(fname, g_results, genids, gbuses, e_results, exgrid_idx, swsh_idxs, swgen_idxs):
    g_results.drop(swsh_idxs, inplace=True)
    del g_results['vm_pu']
    del g_results['va_degree']
    # -- COMBINE SWING GENERATOR AND EXTERNAL GRID CONTRIBUTIONS --------------
    ex_kw = e_results.loc[exgrid_idx, 'p_kw']
    ex_kvar = e_results.loc[exgrid_idx, 'q_kvar']
    for idx in swgen_idxs:
        g_results.loc[idx, 'p_kw'] += ex_kw / len(swgen_idxs)
        g_results.loc[idx, 'q_kvar'] += ex_kvar / len(swgen_idxs)
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
    return


def write_bus_results(fname, b_results, sw_dict, g_results, clabel, exgridbus):
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
        if bus in sw_dict:
            mvars = -1e-3 * g_results.loc[sw_dict[bus], 'q_kvar']
            buslist[j][3] = mvars + 0.0
    # -- WRITE THE BUS RESULTS TO FILE ----------------------------------------
    write_csvdata(fname, [], [['--contingency'], ['label'], [clabel]])
    write_csvdata(fname, buslist, [['--bus section']])
    return


def write_gen_results(fname, g_results, genids, gbuses, b_pgens, e_results, exgrid_idx, swsh_idxs, swgen_idxs, pgen_out):
    g_results.drop(swsh_idxs, inplace=True)
    del g_results['vm_pu']
    del g_results['va_degree']
    # -- COMBINE SWING GENERATOR AND EXTERNAL GRID CONTRIBUTIONS --------------
    ex_kw = e_results.loc[exgrid_idx, 'p_kw']
    ex_kvar = e_results.loc[exgrid_idx, 'q_kvar']
    for idx in swgen_idxs:
        g_results.loc[idx, 'p_kw'] += ex_kw / len(swgen_idxs)
        g_results.loc[idx, 'q_kvar'] += ex_kvar / len(swgen_idxs)
    # -- CONVERT BACK TO MW AND MVARS -----------------------------------------
    g_results['p_kw'] *= -1e-3
    g_results['q_kvar'] *= -1e-3
    g_results['p_kw'] += 0.0
    g_results['q_kvar'] += 0.0
    b_pgens *= -1e-3
    pgen_out *= -1e-3
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
    deltapgens = c_gens - b_pgens + pgen_out
    write_csvdata(fname, [], [['--delta section'], ['delta_p'], [deltapgens]])
    return


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
                lol[i] = [-1e3 * float(lol[i][0]), float(lol[i][1])]
                cdict[ckey].append(lol[i])
    for ckey in cdict:
        # cdict[ckey] = tupleoftuples(cdict[ckey])
        # cdict[ckey] = list(set(cdict[ckey]))
        # cdict[ckey] = listoflists(cdict[ckey])
        cdict[ckey].sort()
        cdict[ckey][0][0] -= 1.0
        cdict[ckey][-1][0] += 1.0
        for j in range(1, len(cdict[ckey])):
            cdict[ckey][j][0] = cdict[ckey][j][0] + 0.1 * j
    return cdict


def print_dataframes_results(_net):
    pdoptions.display.max_columns = 1000
    pdoptions.display.max_rows = 1000
    pdoptions.display.max_colwidth = 199
    pdoptions.display.width = None
    pdoptions.display.precision = 4
    # print()
    # print('BUS DATAFRAME')
    # print(_net.bus)
    # print()
    # print('BUS RESULTS')
    # print(_net.res_bus)
    # print()
    # print('LINE DATAFRAME')
    # print(_net.line)
    # print()
    print('LINE RESULTS')
    print(_net.res_line)
    print()
    # print('TRANSFORMER DATAFRAME')
    # print(_net.trafo)
    # print()
    print('TRANSFORMER RESULTS')
    print(_net.res_trafo)
    print()
    print('GENERATOR DATAFRAME')
    print(_net.gen)
    print()
    print('GENERATOR RESULTS')
    print(_net.res_gen)
    # print()
    # print('EXT GRID DATAFRAME')
    # print(_net.ext_grid)
    print()
    print('EXT GRID RESULTS')
    print(_net.res_ext_grid)
    print()
    return


def get_branch_losses(line_res,  trafo_res):
    losses = 0.0
    pfrom = line_res['p_from_kw'].values
    pto = net.res_line['p_to_kw'].values
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


# =============================================================================
# -- MAIN ---------------------------------------------------------------------
# =============================================================================
if __name__ == "__main__":
    cwd = os.getcwd()
    start_time = time.time()

    # =========================================================================
    # -- PARSE THE RAW FILE ---------------------------------------------------
    # =========================================================================
    print()
    print('GETTING RAW DATA FROM FILE .........................................', os.path.split(raw_fname)[1])
    mva_base, basefreq, raw_busdata, raw_loaddata, raw_fixshuntdata, raw_gendata, raw_branchdata, raw_xfmrdata, raw_swshuntdata = get_raw_data(raw_fname)
    if not raw_fixshuntdata: raw_fixshuntdata = [[]]
    if not raw_swshuntdata: raw_swshuntdata = [[]]
    # -- SEPARATE 2WXFMRS AND 3WXFMRS -----------------------------------------
    raw_xfmr2wdata, raw_xfmr3wdata = split_xfmrdata(raw_xfmrdata)
    if not raw_xfmr2wdata: raw_xfmr2wdata = [[]]
    if not raw_xfmr3wdata: raw_xfmr3wdata = [[]]
    # -- ASSIGN DATA TYPES TO RAW DATA ----------------------------------------
    busdata, areas = format_busdata(raw_busdata)
    loaddata = format_loaddata(raw_loaddata)
    fixshuntdata = format_fixshuntdata(raw_fixshuntdata)
    gendata = format_gendata(raw_gendata)
    branchdata = format_branchdata(raw_branchdata)
    xfmr2wdata = format_xfmr2wdata(raw_xfmr2wdata)
    swshuntdata = format_swshuntdata(raw_swshuntdata)
    # -- GET SWING BUS FROM RAW BUSDATA ---------------------------------------
    swingbus, swing_name, swing_kv, swing_angle, swing_vlow, swing_vhigh = get_swingbus_data(raw_busdata)
    # -- GET SWING GEN DATA FROM GENDATA (REMOVE SWING GEN FROM GENDATA) ------
    gendata, swinggens_data = get_swing_gen_data(gendata, swingbus)

    # =========================================================================
    # -- PARSE CON FILE -------------------------------------------------------
    # =========================================================================
    print('GETTING CONTINGENCY DATA FROM FILE .................................', os.path.split(con_fname)[1])
    outagedict = get_contingencies(con_fname)

    # =========================================================================
    # -- PARSE THE ROP FILE ---------------------------------------------------
    # =========================================================================
    print('GETTING GENERATOR OPF DATA FROM FILE ...............................', os.path.split(rop_fname)[1])
    rop_gendispdata, rop_powerdispdata, rop_pwlcostdata = get_rop_data(rop_fname)
    if not rop_gendispdata: rop_gendispdata = [[]]
    if not rop_powerdispdata: rop_powerdispdata = [[]]
    if not rop_pwlcostdata: rop_pwlcostdata = [[]]
    # -- ASSIGN DATA TYPES TO ROP DATA AND CONVERT TO DICTS -------------------
    genopfdict = format_gendispdata(rop_gendispdata)
    gdispdict = format_powerdispdata(rop_powerdispdata)
    pwlcostdata = format_pwlcostdata(rop_pwlcostdata)

    # =========================================================================
    # -- PARSE THE INL FILE ---------------------------------------------------
    # =========================================================================
    print('GETTING GENERATOR PARTICIPATION FACTORS FROM FILE ..................', os.path.split(inl_fname)[1])
    participation_dict = get_gen_reserves(inl_fname)

    # =========================================================================
    # == CREATE NETWORK =======================================================
    # =========================================================================
    print('========================= CREATING NETWORK =========================')
    create_starttime = time.time()
    kva_base = 1000 * mva_base
    net_a = pp.create_empty_network('net', basefreq, kva_base)
    net_c = pp.create_empty_network('net', basefreq, kva_base)

    # == ADD BUSES TO NETWORK =================================================
    # bus = ['I', 'NAME', 'BASKV', 'IDE', 'AREA', 'ZONE', 'OWNER', 'VM', 'VA', 'NVHI', 'NVLO', 'EVHI', 'EVLO']
    print('ADD BUSES ..........................................................')
    busnomkvdict = {}
    buskvdict = {}
    busarea_dict = {}
    buszone_dict = {}
    for data in busdata:
        busnum = data[0]
        busname = data[1]
        busnomkv = data[2]
        status = abs(data[3]) < 4
        busarea = data[4]
        buszone = data[5]
        buskv = data[7]
        vmax = data[11]
        vmin = data[12]
        pp.create_bus(net_a, vn_kv=busnomkv, name=busname, index=busnum, type="b", zone=buszone, in_service=status, max_vm_pu=vmax, min_vm_pu=vmin)
        idx = pp.create_bus(net_c, vn_kv=busnomkv, name=busname, index=busnum, type="b", zone=buszone, in_service=status, max_vm_pu=vmax, min_vm_pu=vmin)
        if busnum == swingbus:
            swingbus_idx = idx
        busnomkvdict.update({busnum: busnomkv})
        buskvdict.update({busnum: buskv})
        busarea_dict.update({busnum: busarea})
        buszone_dict.update({busnum: buszone})

    # == ADD LOADS TO NETWORK =================================================
    print('ADD LOADS ..........................................................')
    # load = ['I', 'ID', 'STATUS', 'AREA', 'ZONE', 'PL', 'QL', 'IP', 'IQ', 'YP', 'YQ', 'OWNER', 'SCALE', 'INTRPT']
    for ldata in loaddata:
        status = bool(ldata[2])
        if not status:
            continue
        loadbus = ldata[0]
        loadid = ldata[1]
        loadname = str(loadbus) + '-' + loadid
        loadp = ldata[5] * 1e3
        loadq = ldata[6] * 1e3
        pp.create_load(net_a, loadbus, loadp, q_kvar=loadq, name=loadname,
                       max_p_kw=loadp, min_p_kw=loadp, max_q_kvar=loadq, min_q_kvar=loadq, controllable=False, in_service=status)
        pp.create_load(net_c, loadbus, loadp, q_kvar=loadq, name=loadname,
                       max_p_kw=loadp, min_p_kw=loadp, max_q_kvar=loadq, min_q_kvar=loadq, controllable=False, in_service=status)

    # == ADD GENERATORS TO NETWORK ============================================
    # gens = ['I', 'ID', 'PG', 'QG', 'QT', 'QB', 'VS', 'IREG', 'MBASE', 'ZR', 'ZX', 'RT', 'XT', 'GTAP', 'STAT', 'RMPCT', 'PT', 'PB',
    #         'O1', 'F1', 'O2', 'F2', 'O3', 'F3', 'O4', 'F4', 'WMOD', 'WPF']
    print('ADD GENERATORS .....................................................')
    genbuses = []
    gids = []
    genidxdict = {}
    swinggen_idxs = []
    pfactor_dict = {}
    gen_status_vreg_dict = {}
    genbus_dict = {}
    genarea_dict = {}
    genzone_dict = {}
    # -- ADD SWING GENERATOR --------------------------------------------------
    # -- swinggens_data = [sw_key, sw_id, vreg_sw, sw_pgen, sw_pmin, sw_pmax, sw_qgen, sw_qmin, sw_qmax, sw_status]
    for swgen_data in swinggens_data:
        genbus = swingbus
        genkey = swgen_data[0]
        gid = swgen_data[1]
        vreg = swgen_data[2]
        pgen = swgen_data[3]
        pmin = swgen_data[4]
        pmax = swgen_data[5]
        qgen = swgen_data[6]
        qmin = swgen_data[7]
        qmax = swgen_data[8]
        status = bool(swgen_data[9])
        genkva = math.sqrt(pmin **2 + qmin ** 2)
        gen_status_vreg_dict.update({genbus: [False, vreg]})
        pcostdata = None
        if genkey in genopfdict:
            disptablekey = genopfdict[genkey]
            costtablekey = gdispdict[disptablekey]
            pcostdata = numpy.array(pwlcostdata[costtablekey])
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, name=genkey, min_p_kw=pmin, max_p_kw=pmax, min_q_kvar=qmin, max_q_kvar=qmax,
                                controllable=True, in_service=status, type='sync', sn_kva=genkva)
            pp.create_piecewise_linear_cost(net_a, idx, 'gen', pcostdata, type='p')
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, name=genkey, min_p_kw=pmin, max_p_kw=pmax, min_q_kvar=qmin, max_q_kvar=qmax,
                          controllable=True, in_service=status, type='sync', index=idx, sn_kva=genkva)
            pp.create_piecewise_linear_cost(net_c, idx, 'gen', pcostdata, type='p')

            if genkey in participation_dict:
                pfactor = participation_dict[genkey]
                pfactor_dict.update({genkey: pfactor})
        else:
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, name=genkey, min_p_kw=pmin, max_p_kw=pmax, min_q_kvar=qmin, max_q_kvar=qmax,
                                controllable=False, in_service=status, type='sync', sn_kva=genkva)
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, name=genkey, min_p_kw=pmin, max_p_kw=pmax, min_q_kvar=qmin, max_q_kvar=qmax,
                          controllable=False, in_service=status, type='sync', index=idx, sn_kva=genkva)

        swing_vreg = vreg
        if status:
            gen_status_vreg_dict[genbus][0] = status
        swinggen_idxs.append(idx)
        genidxdict.update({genkey: idx})
        genbuses.append(genbus)
        gids.append("'" + gid + "'")
        genbus_dict.update({genkey: genbus})
        genarea_dict.update({genkey: busarea_dict[genbus]})
        genzone_dict.update({genkey: buszone_dict[genbus]})

    # -- ADD REMAINING GENERATOR ----------------------------------------------
    for data in gendata:
        genbus = data[0]
        gid = data[1]
        pgen = data[2]
        qgen = data[3]
        qmin = data[4]
        qmax = data[5]
        vreg = data[6]
        pmin = data[16]
        pmax = data[17]
        status = bool(data[14])
        genkva = math.sqrt(pmin **2 + qmin ** 2)
        gen_status_vreg_dict.update({genbus: [False, vreg]})
        pcostdata = None
        genkey = str(genbus) + '-' + gid
        if genkey in genopfdict:
            disptablekey = genopfdict[genkey]
            costtablekey = gdispdict[disptablekey]
            pcostdata = numpy.array(pwlcostdata[costtablekey])
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, name=genkey, min_p_kw=pmin, max_p_kw=pmax, min_q_kvar=qmin, max_q_kvar=qmax,
                                controllable=True, in_service=status, type='sync', sn_kva=genkva)
            pp.create_piecewise_linear_cost(net_a, idx, 'gen', pcostdata, type='p')
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, name=genkey, min_p_kw=pmin, max_p_kw=pmax, min_q_kvar=qmin, max_q_kvar=qmax,
                          controllable=True, in_service=status, type='sync', index=idx, sn_kva=genkva)
            pp.create_piecewise_linear_cost(net_c, idx, 'gen', pcostdata, type='p')

            if genkey in participation_dict:
                pfactor = participation_dict[genkey]
                pfactor_dict.update({genkey: pfactor})
        else:
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, name=genkey, min_p_kw=pmin, max_p_kw=pmax, min_q_kvar=qmin, max_q_kvar=qmax,
                                controllable=False, in_service=status, type='sync', sn_kva=genkva)
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, name=genkey, min_p_kw=pmin, max_p_kw=pmax, min_q_kvar=qmin, max_q_kvar=qmax,
                          controllable=False, in_service=status, type='sync', index=idx, sn_kva=genkva)

        if status:
            gen_status_vreg_dict[genbus][0] = status
        gids.append("'" + gid + "'")
        genidxdict.update({genkey: idx})
        genbuses.append(genbus)
        genbus_dict.update({genkey: genbus})
        genarea_dict.update({genkey: busarea_dict[genbus]})
        genzone_dict.update({genkey: buszone_dict[genbus]})

    # == ADD FIXED SHUNT DATA TO NETWORK ======================================
    # fixshunt = ['I', 'ID', 'STATUS', 'GL', 'BL']
    fxidxdict = {}
    if fixshuntdata != [[]]:
        print('ADD FIXED SHUNTS ...................................................')
        for data in fixshuntdata:
            status = bool(data[2])
            if not status:
                continue
            shuntbus = data[0]
            shuntname = str(shuntbus) + '-FX'
            kw = data[3]
            kvar = data[4]
            idx = pp.create_shunt(net_a, shuntbus, kvar, p_kw=kw, step=1, max_step=True, name=shuntname)
            pp.create_shunt(net_c, shuntbus, kvar, p_kw=kw, step=1, max_step=True, name=shuntname, index=idx)
            fxidxdict.update({shuntbus: idx})

    # == ADD SWITCHED SHUNTS TO NETWORK =======================================
    # -- SWSHUNTS ARE MODELED AS Q-GENERATORS ---------------------------------
    # swshunt = ['I', 'MODSW', 'ADJM', 'STAT', 'VSWHI', 'VSWLO', 'SWREM', 'RMPCT', 'RMIDNT', 'BINIT', 'N1', 'B1',
    #            'N2', 'B2', 'N3', 'B3', 'N4', 'B4', 'N5', 'B5', 'N6', 'B6', 'N7', 'B7', 'N8', 'B8']
    # gens = ['I', 'ID', 'PG', 'QG', 'QT', 'QB', 'VS', 'IREG', 'MBASE', 'ZR', 'ZX', 'RT', 'XT', 'GTAP', 'STAT', 'RMPCT', 'PT', 'PB',
    #         'O1', 'F1', 'O2', 'F2', 'O3', 'F3', 'O4', 'F4', 'WMOD', 'WPF']
    swshidxdict = {}
    swshidxs = []
    if swshuntdata != [[]]:
        print('ADD SWITCHED SHUNTS ................................................')
        for data in swshuntdata:
            status = bool(data[3])
            if not status:
                continue
            shuntbus = data[0]
            vreg = buskvdict[shuntbus]
            if shuntbus in gen_status_vreg_dict:
                if gen_status_vreg_dict[shuntbus][0]:
                    vreg = gen_status_vreg_dict[shuntbus][1]
            swshkey = str(shuntbus) + '-SW'
            steps = [data[10], data[12], data[14], data[16], data[18], data[20], data[22], data[24]]
            kvars = [data[11], data[13], data[15], data[17], data[19], data[21], data[23], data[25]]
            total_qmin = 0.0
            total_qmax = 0.0
            for j in range(len(kvars)):
                if kvars[j] < 0.0:
                    total_qmin += steps[j] * kvars[j]
                elif kvars[j] > 0.0:
                    total_qmax += steps[j] * kvars[j]
            idx = pp.create_gen(net_a, shuntbus, 0.0, vm_pu=vreg, min_q_kvar=total_qmin, max_q_kvar=total_qmax,
                                min_p_kw=0.0, max_p_kw=0.0, controllable=False, name=swshkey, type='swsh')
            pp.create_gen(net_c, shuntbus, 0.0, vm_pu=vreg, min_q_kvar=total_qmin, max_q_kvar=total_qmax,
                          min_p_kw=0.0, max_p_kw=0.0, controllable=False, name=swshkey, type='swsh', index=idx)
            swshidxdict.update({shuntbus: idx})
            swshidxs.append(idx)

    # == ADD LINES TO NETWORK =================================================
    # branch = ['I', 'J', 'CKT', 'R', 'X', 'B', 'RATEA', 'RATEB', 'RATEC', 'GI', 'BI', 'GJ', 'BJ', 'ST', 'MET', 'LEN',
    #           'O1', 'F1', 'O2', 'F2', 'O3', 'F3', 'O4', 'F4']
    linedict = {}
    line_ratea_dict = {}
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
            kv = busnomkvdict[frombus]
            zbase = kv ** 2 / mva_base
            r_pu = data[3] / length
            x_pu = data[4] / length
            b_pu = data[5] / length
            r = r_pu * zbase
            x = x_pu * zbase
            b = b_pu / zbase
            capacitance = 1e9 * b / (2 * math.pi * basefreq)
            mva_rating_a = data[6]
            mva_rating_b = data[7]
            mva_rating_c = data[8]
            if BASERATING == 0:
                base_mva_rating = mva_rating_a
            elif BASERATING == 1:
                base_mva_rating = mva_rating_b
            elif BASERATING == 2:
                base_mva_rating = mva_rating_c
            if CONRATING == 0:
                mva_rating = mva_rating_a
            elif CONRATING == 1:
                mva_rating = mva_rating_b
            elif CONRATING == 2:
                mva_rating = mva_rating_c
            i_rating_a = base_mva_rating / (math.sqrt(3) * kv)
            i_rating_c = mva_rating / (math.sqrt(3) * kv)

            linekey = str(frombus) + '-' + str(tobus) + '-' + ckt
            idx = pp.create_line_from_parameters(net_a, frombus, tobus, length, r, x, capacitance, i_rating_a, name=linekey,
                                                 in_service=status, max_loading_percent=100.0)
            pp.create_line_from_parameters(net_c, frombus, tobus, length, r, x, capacitance, i_rating_c, name=linekey,
                                           in_service=status, max_loading_percent=100.0, index=idx)
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
            fromkv = busnomkvdict[frombus]
            tokv = busnomkvdict[tobus]
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
            if BASERATING == 0:
                base_mva_rating = mva_rating_a
            elif BASERATING == 1:
                base_mva_rating = mva_rating_b
            elif BASERATING == 2:
                base_mva_rating = mva_rating_c
            if CONRATING == 0:
                mva_rating = mva_rating_a
            elif CONRATING == 1:
                mva_rating = mva_rating_b
            elif CONRATING == 2:
                mva_rating = mva_rating_c
            ra_pu = r_pu * base_mva_rating / mva_base      # pandapower uses given transformer rating as test mva
            xa_pu = x_pu * base_mva_rating / mva_base      # so convert to mva_rating base
            za_pu = math.sqrt(ra_pu ** 2 + xa_pu ** 2)  # calculate 'nameplate' pu impedance
            za_pct = 100.0 * za_pu                      # pandadower uses percent impedance
            ra_pct = 100.0 * ra_pu                      # pandadower uses percent resistance
            kva_rating_a = 1e3 * base_mva_rating           # rate a for base case analysis

            rc_pu = r_pu * mva_rating / mva_base        # pandapower uses given transformer rating as test mva
            xc_pu = x_pu * mva_rating / mva_base        # so convert to mva_rating base
            zc_pu = math.sqrt(rc_pu ** 2 + xc_pu ** 2)  # calculate 'nameplate' pu impedance
            zc_pct = 100.0 * zc_pu                      # pandadower uses percent impedance
            rc_pct = 100.0 * rc_pu                      # pandadower uses percent resistance
            kva_rating_c = 1e3 * mva_rating             # rate c for contingency analysis

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
            idx = pp.create_transformer_from_parameters(net_a, frombus, tobus, kva_rating_a, fromkv, tokv, ra_pct, za_pct, ironlosses, noloadlosses, shift_degree=phaseshift,
                                                        tp_side=tapside, tp_mid=tapmid, tp_max=tapmax, tp_min=tapmin, tp_st_percent=tap_pct, tp_pos=tappos, in_service=status,
                                                        max_loading_percent=100.0, name=xfmr2wkey)
            pp.create_transformer_from_parameters(net_c, frombus, tobus, kva_rating_c, fromkv, tokv, rc_pct, zc_pct, ironlosses, noloadlosses, shift_degree=phaseshift,
                                                  tp_side=tapside, tp_mid=tapmid, tp_max=tapmax, tp_min=tapmin, tp_st_percent=tap_pct, tp_pos=tappos, in_service=status,
                                                  max_loading_percent=100.0, name=xfmr2wkey, index=idx)
            xfmrdict.update({xfmr2wkey: idx})
            xfmr_ratea_dict.update({xfmr2wkey: kva_rating_a})

    # == ADD EXTERNAL GRID (PARALLEL TO SWING BUS) ============================
    ext_grid_idx = pp.create_bus(net_a, vn_kv=swing_kv, name='Ex_Grid_Bus', in_service=True, max_vm_pu=swing_vhigh, min_vm_pu=swing_vlow)
    pp.create_bus(net_c, vn_kv=swing_kv, name='Ex_Grid_Bus', in_service=True, max_vm_pu=swing_vhigh, min_vm_pu=swing_vlow, index=ext_grid_idx)
    ext_tie_rating = 1e5/(math.sqrt(3) * swing_kv)
    tie_idx = pp.create_line_from_parameters(net_a, swingbus, ext_grid_idx, 1.0, 0.0, 0.002, 0.0, ext_tie_rating, name='Swing-Tie')
    pp.create_line_from_parameters(net_c, swingbus, ext_grid_idx, 1.0, 0.0, 0.002, 0.0, ext_tie_rating, name='Swing-Tie', index=tie_idx)
    pp.create_ext_grid(net_a, ext_grid_idx, vm_pu=swing_vreg, va_degree=swing_angle, min_p_kw=-1e9, max_p_kw=0.0,
                       min_q_kvar=-1e9, max_q_kvar=0.0, index=ext_grid_idx)
    pp.create_ext_grid(net_c, ext_grid_idx, vm_pu=swing_vreg, va_degree=swing_angle, min_p_kw=-1e9, max_p_kw=0.0,
                       min_q_kvar=-1e9, max_q_kvar=0.0, index=ext_grid_idx)
    pp.create_polynomial_cost(net_a, ext_grid_idx, 'ext_grid', numpy.array([-1, 0]), type='p')
    pp.create_polynomial_cost(net_c, ext_grid_idx, 'ext_grid', numpy.array([-1, 0]), type='p')

    pp.create_polynomial_cost(net_a, ext_grid_idx, 'ext_grid', numpy.array([1, 0]), type='q')
    pp.create_polynomial_cost(net_c, ext_grid_idx, 'ext_grid', numpy.array([1, 0]), type='q')

    print('====================== DONE CREATING NETWORK =======================', round(time.time() - create_starttime, 3))

    # -- DIAGNOSTIC DEVELOPMENT -----------------------------------------------
    # pp.diagnostic(net_a, report_style='detailed', warnings_only=False)

    try:
        os.remove(outfname1)
    except FileNotFoundError:
        pass
    try:
        os.remove(outfname2)
    except FileNotFoundError:
        pass

    print('SOLVING NETWORK ....................................................', end=' ', flush=True)
    solve_starttime = time.time()
    pp.runpp(net_a, init='auto', max_iteration=ITMXN, calculate_voltage_angles=True, enforce_q_lims=True)
    net_a.gen['p_kw'] = net_a.res_gen['p_kw']
    pp.runpp(net_c, init='auto', max_iteration=ITMXN, calculate_voltage_angles=True, enforce_q_lims=True)
    net_c.gen['p_kw'] = net_c.res_gen['p_kw']
    print(round(time.time() - solve_starttime, 3))

    # =========================================================================
    # -- PROCESS BASECASE OPTIMAL POWER FLOW ----------------------------------
    # =========================================================================
    print('SOLVING BASECASE RATEA OPTIMAL POWER FLOW ..........................', end=' ', flush=True)
    solve_starttime = time.time()
    pp.runopp(net_a, init='flat', calculate_voltage_angles=True, verbose=False, suppress_warnings=True, enforce_q_lims=False)
    print(round(time.time() - solve_starttime, 3))
    # print_dataframes_results(net_a)

    # -- GET TOTAL BASECASE GENERATION ----------------------------------------
    ex_pgen = net_a.res_ext_grid.loc[ext_grid_idx, 'p_kw']
    base_pgens = sum([x for x in net_a.res_gen['p_kw'].values]) + ex_pgen

    # -- WRITE BASECASE BUS AND GENERATOR RESULTS TO FILE ---------------------
    bus_results = copy.deepcopy(net_a.res_bus)
    gen_results = copy.deepcopy(net_a.res_gen)
    ext_grid_results = net_a.res_ext_grid
    write_base_bus_results(outfname1, bus_results, swshidxdict, gen_results, ext_grid_idx)
    write_base_gen_results(outfname1, gen_results, gids, genbuses, ext_grid_results, ext_grid_idx, swshidxs, swinggen_idxs)

    # -- RUN & COPY RATEC BASE CASE NETWORK FOR CONTINGENCY INITIALIZATION ----
    print('SOLVING BASECASE RATEC OPTIMAL POWER FLOW ..........................', end=' ', flush=True)
    solve_starttime = time.time()
    pp.runopp(net_c, init='flat', calculate_voltage_angles=True, verbose=False, suppress_warnings=True)
    net_c.gen['p_kw'] = net_c.res_gen['p_kw']
    base_net = copy.deepcopy(net_c)
    print(round(time.time() - solve_starttime, 3))

    # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
    # -- PROCESS STRAIGHT POWER FLOW CONTINGENCIES (FOR PGEN ESTIMATE)  ---
    # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
    print('--------------------- CONTINGENCY POWER FLOW -----------------------')
    deltap_dict = {}
    if outagedict['gen']:
        print('RUNNING GENERATOR OUTAGES ..........................................', end=' ', flush=True)
        gstarttime = time.time()
        for genkey in outagedict['gen']:
            net = copy.deepcopy(base_net)
            conlabel = outagedict['gen'][genkey]
            if genkey in genidxdict:
                genidx = genidxdict[genkey]
                pgen_outage = net.res_gen.loc[genidx, 'p_kw']
                net.gen.in_service[genidx] = False
            else:
                print('GENERATOR NOT FOUND ................................................', conlabel)
                continue
            pp.runpp(net, init='auto', max_iteration=ITMXN, calculate_voltage_angles=True, enforce_q_lims=True)

            # -- CALCULATE CHANGE IN GENERATION COMPARED TO BASECASE  -------------
            ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_kw']
            con_pgens = sum([x for x in net.res_gen['p_kw'].values]) + ex_pgen
            deltap_dict.update({genkey: con_pgens - base_pgens + pgen_outage})

            # -- TODO not needed in production
            # bus_results = copy.deepcopy(net.res_bus)
            # gen_results = copy.deepcopy(net.res_gen)
            # ext_grid_results = net.res_ext_grid
            # # -- WRITE CONTINGENCY BUS AND GENERATOR RESULTS TO FILE --------------
            # conlabel = "'" + outagedict['gen'][genkey] + "'"
            # write_bus_results('straight_solution2', bus_results, swshidxdict, gen_results, conlabel, ext_grid_idx)
            # write_gen_results('straight_solution2', gen_results, gids, genbuses, base_pgens, ext_grid_results, ext_grid_idx, swshidxs, swinggen_idxs, pgen_outage)
        print(round(time.time() - gstarttime, 3))

    if outagedict['branch']:
        print('RUNNING LINE AND TRANSFORMER OUTAGES ...............................', end=' ', flush=True)
        lxstarttime = time.time()
        for branchkey in outagedict['branch']:
            net = copy.deepcopy(base_net)
            conlabel = outagedict['branch'][branchkey]
            if branchkey in linedict:
                lineidx = linedict[branchkey]
                net.line.in_service[lineidx] = False
            elif branchkey in xfmrdict:
                xfmridx = xfmrdict[branchkey]
                net.trafo.in_service[xfmridx] = False
            else:
                print('LINE OR TRANSFORMER NOT FOUND ......................................', branchkey)
                continue
            pp.runpp(net, init='auto', max_iteration=ITMXN, calculate_voltage_angles=True)

            # -- CALCULATE CHANGE IN GENERATION COMPARED TO BASECASE  -------------
            ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_kw']
            con_pgens = sum([x for x in net.res_gen['p_kw'].values]) + ex_pgen
            deltap_dict.update({branchkey: con_pgens - base_pgens})

            # -- TODO not needed in production
            # bus_results = copy.deepcopy(net.res_bus)
            # gen_results = copy.deepcopy(net.res_gen)
            # ext_grid_results = net.res_ext_grid
            # # -- WRITE CONTINGENCY BUS AND GENERATOR RESULTS TO FILE ----------
            # conlabel = "'" + outagedict['branch'][branchkey] + "'"
            # write_bus_results('straight_solution2', bus_results, swshidxdict, gen_results, conlabel, ext_grid_idx)
            # write_gen_results('straight_solution2', gen_results, gids, genbuses, base_pgens, ext_grid_results, ext_grid_idx, swshidxs, swinggen_idxs, 0.0)
        print(round(time.time() - lxstarttime, 3))

    # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
    # -- PROCESS OPF CONTINGENCIES ----------------------------------------
    # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
    print('------------------ CONTINGENCY OPTIMAL POWER FLOW ------------------')
    opf_starttime = time.time()
    if outagedict['gen']:
        print('RUNNING OPF GENERATOR OUTAGES ......................................')
        for genkey in outagedict['gen']:
            net = copy.deepcopy(base_net)
            conlabel = outagedict['gen'][genkey]
            if genkey in genidxdict:
                genidx = genidxdict[genkey]
                pgen_outage = net.res_gen.loc[genidx, 'p_kw']
                net.gen.in_service[genidx] = False
            else:
                print('GENERATOR NOT FOUND ................................................', conlabel)
                continue

            # -- SET GENERATOR LIMITS ACCORDING TO PFACTOR --------------------
            deltap_estimate = deltap_dict[genkey]

            for gen_pkey in participation_dict:
                gidx = genidxdict[gen_pkey]
                pfactor = participation_dict[gen_pkey]
                pgen = net.res_gen.loc[gidx, 'p_kw']
                pmin = net.gen.loc[gidx, 'min_p_kw']
                pmax = net.gen.loc[gidx, 'max_p_kw']
                p_limit = pgen + pfactor * deltap_estimate

                if deltap_estimate > 0.0:
                    net.gen.loc[gidx, 'max_p_kw'] = min(pmax, p_limit)
                    # if p_limit > pmax:
                    #     print('OUTAGE = ', genkey)
                    #     print('PARTICIPATING GENERATOR =', gen_pkey, 'PGEN =', pgen)
                    #     print('PFACTOR =', pfactor, 'DELTAP ESTIMATE =', deltap_estimate)
                    #     print('REQUESTED PMAX =', p_limit)
                    #     print('PMIN =',  pmin)
                    #     print('PMAX =',  pmax)
                    #     print('PMAX_LIMIT =', min(pmax, p_limit))

                elif deltap_estimate < 0.0:
                    net.gen.loc[gidx, 'min_p_kw'] = max(pmin, p_limit)
                    # if p_limit < pmin:
                    #     print('PARTICIPATING GENERATOR =', gen_pkey, 'PGEN =', pgen)
                    #     print('PFACTOR =', pfactor, 'DELTAP ESTIMATE =', deltap_estimate)
                    #     print('REQUESTED PMIN =',  p_limit)
                    #     print('PMIN =', pmin)
                    #     print('PMAX =', pmax)
                    #     print('PMIN_LIMIT =', max(pmin, p_limit))

            pp.runopp(net, init='flat', calculate_voltage_angles=True, verbose=False, suppress_warnings=True)

            # -- WRITE CONTINGENCY BUS AND GENERATOR RESULTS TO FILE ----------
            bus_results = copy.deepcopy(net.res_bus)
            gen_results = copy.deepcopy(net.res_gen)
            ext_grid_results = net.res_ext_grid
            # -- WRITE CONTINGENCY BUS AND GENERATOR RESULTS TO FILE --------------
            conlabel = "'" + outagedict['gen'][genkey] + "'"
            write_bus_results(outfname2, bus_results, swshidxdict, gen_results, conlabel, ext_grid_idx)
            write_gen_results(outfname2, gen_results, gids, genbuses, base_pgens, ext_grid_results, ext_grid_idx, swshidxs, swinggen_idxs, pgen_outage)

    if outagedict['branch']:
        print('RUNNING OPF LINE AND TRANSFORMER OUTAGES ...........................')
        for branchkey in outagedict['branch']:
            net = copy.deepcopy(base_net)
            conlabel = outagedict['branch'][branchkey]
            if branchkey in linedict:
                lineidx = linedict[branchkey]
                net.line.in_service[lineidx] = False
            elif branchkey in xfmrdict:
                xfmridx = xfmrdict[branchkey]
                net.trafo.in_service[xfmridx] = False
            else:
                print('LINE OR TRANSFORMER NOT FOUND ......................................', conlabel)
                continue

            # -- TODO limit pmax and pmin according to pfactor here then run

            pp.runopp(net, init='flat', calculate_voltage_angles=True, verbose=False, suppress_warnings=True)

            # -- WRITE CONTINGENCY BUS AND GENERATOR RESULTS TO FILE ----------
            bus_results = copy.deepcopy(net.res_bus)
            gen_results = copy.deepcopy(net.res_gen)
            ext_grid_results = net.res_ext_grid
            # -- WRITE CONTINGENCY BUS AND GENERATOR RESULTS TO FILE ----------
            conlabel = "'" + outagedict['branch'][branchkey] + "'"
            write_bus_results(outfname2, bus_results, swshidxdict, gen_results, conlabel, ext_grid_idx)
            write_gen_results(outfname2, gen_results, gids, genbuses, base_pgens, ext_grid_results, ext_grid_idx, swshidxs, swinggen_idxs, 0.0)

    print('DONE WITH OPF CONTINGENCIES ........................................', round(time.time() - opf_starttime, 3))
    print('')
    print('TOTAL TIME -------------------------------------------------------->', round(time.time() - start_time, 3))
