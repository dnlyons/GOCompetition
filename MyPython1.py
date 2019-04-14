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
import traceback
from io import StringIO
# import data as data_read


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
    #neta_fname = os.path.join('submission', 'neta.p')
    #netc_fname = os.path.join('submission', 'netc.p')
    #data_fname = os.path.join('submission', 'netdata.pkl')
    neta_fname = os.path.abspath('..') + r'/neta.p'
    netc_fname = os.path.abspath('..') + r'/netc.p'
    data_fname = os.path.abspath('..') + r'/netdata.pkl'

# -----------------------------------------------------------------------------
# -- DEVELOPMENT --- DEVELOPMENT --- DEVELOPMENT --- DEVELOPMENT --------------
# -----------------------------------------------------------------------------
if not sys.argv[1:]:
    con_fname = cwd + r'/sandbox/Network_01R-10/scenario_1/case.con'
    inl_fname = cwd + r'/sandbox/Network_01R-10/case.inl'
    raw_fname = cwd + r'/sandbox/Network_01R-10/scenario_1/case.raw'
    rop_fname = cwd + r'/sandbox/Network_01R-10/case.rop'
    outfname1 = cwd + r'/sandbox/Network_01R-10/scenario_1/solution1.txt'
    outfname2 = cwd + r'/sandbox/Network_01R-10/scenario_1/solution2.txt'

    neta_fname = cwd + r'/sandbox/Network_01R-10/scenario_1/neta.p'
    netc_fname = cwd + r'/sandbox/Network_01R-10/scenario_1/netc.p'
    data_fname = cwd + r'/sandbox/Network_01R-10/scenario_1/netdata.pkl'

    try:
        os.remove(outfname1)
        os.remove(outfname2)
    except FileNotFoundError:
        pass


GVREG = 1                   # NON-SWING GENERATORS VOLTAGE SCHEDULES ... 0=DEFAULT_GENV_RAW, 1=CUSTOM(ALL)
SWSHVREG = 1                # SWITCHED SHUNTS VOLTAGE SCHEDULES ........ 0=DEFAULT_RAW, 1=CUSTOM(ALL)
Gvreg_Custom = 1.025
SwShVreg_Custom = 1.025

SwVreg_Init = 1.040         # INITIAL SWING GENERATORS VOLTAGE SETPOINT (THEN CALCULATE FOR VAR MARGIN)

# =============================================================================
# -- DATA READ ----------------------------------------------------------------
# =============================================================================
read_unused_fields = True
write_defaults_in_unused_fields = False
write_values_in_unused_fields = True
gen_cost_dx_margin = 1.0e-6  # ensure that consecutive x points differ by at least this amount
gen_cost_ddydx_margin = 1.0e-6  # ensure that consecutive slopes differ by at least this amount
gen_cost_x_bounds_margin = 0.0e-6  # ensure that the pgen lower and upper bounds are covered by at least this amount
raise_extra_field = False  # set to true to raise an exception if extra fields are encountered. This can be a problem if a comma appears in an end-of-line comment.
raise_con_quote = False  # set to true to raise an exception if the con file has quotes. might as well accept this since we are rewriting the files


def alert(alert_dict):
    print(alert_dict)


def parse_token(token, val_type, default=None):
    val = None
    if len(token) > 0:
        val = val_type(token)
    elif default is not None:
        val = val_type(default)
    else:
        try:
            print('required field missing data, token: %s, val_type: %s' % (token, val_type))
            raise Exception('empty field not allowed')
        except Exception as e:
            traceback.print_exc()
            raise e
        # raise Exception('empty field not allowed')
    return val


def pad_row(row, new_row_len):
    try:
        if len(row) != new_row_len:
            if len(row) < new_row_len:
                print('missing field, row:')
                print(row)
                raise Exception('missing field not allowed')
            elif len(row) > new_row_len:
                row = remove_end_of_line_comment_from_row(row, '/')
                if len(row) > new_row_len:
                    alert(
                        {'data_type': 'Data',
                         'error_message': 'extra field, please ensure that all rows have the correcct number of fields',
                         'diagnostics': str(row)})
                    if raise_extra_field:
                        raise Exception('extra field not allowed')
        else:
            row = remove_end_of_line_comment_from_row(row, '/')
    except Exception as e:
        traceback.print_exc()
        raise e
    return row


def check_row_missing_fields(row, row_len_expected):
    try:
        if len(row) < row_len_expected:
            print('missing field, row:')
            print(row)
            raise Exception('missing field not allowed')
    except Exception as e:
        traceback.print_exc()
        raise e


def remove_end_of_line_comment_from_row_first_occurence(row, end_of_line_str):
    index = [r.find(end_of_line_str) for r in row]
    len_row = len(row)
    entries_with_end_of_line_strs = [i for i in range(len_row) if index[i] > -1]
    num_entries_with_end_of_line_strs = len(entries_with_end_of_line_strs)
    if num_entries_with_end_of_line_strs > 0:
        first_entry_with_end_of_line_str = min(entries_with_end_of_line_strs)
        len_row_new = first_entry_with_end_of_line_str + 1
        row_new = [row[i] for i in range(len_row_new)]
        row_new[len_row_new - 1] = remove_end_of_line_comment(row_new[len_row_new - 1], end_of_line_str)
    else:
        row_new = [r for r in row]
    return row_new


def remove_end_of_line_comment_from_row(row, end_of_line_str):
    index = [r.find(end_of_line_str) for r in row]
    len_row = len(row)
    entries_with_end_of_line_strs = [i for i in range(len_row) if index[i] > -1]
    num_entries_with_end_of_line_strs = len(entries_with_end_of_line_strs)
    if num_entries_with_end_of_line_strs > 0:
        # last_entry_with_end_of_line_str = min(entries_with_end_of_line_strs)
        # len_row_new = last_entry_with_end_of_line_str + 1
        row_new = [r for r in row]
        # row_new = [row[i] for i in range(len_row_new)]
        for i in entries_with_end_of_line_strs:
            row_new[i] = remove_end_of_line_comment(row_new[i], end_of_line_str)
        # row_new[len_row_new - 1] = remove_end_of_line_comment(row_new[len_row_new - 1], end_of_line_str)
    else:
        # row_new = [r for r in row]
        row_new = row
    return row_new


def remove_end_of_line_comment(token, end_of_line_str):
    token_new = token
    index = token_new.find(end_of_line_str)
    if index > -1:
        token_new = token_new[0:index]
    return token_new


class Data:
    '''In physical units, i.e. data convention, i.e. input and output data files'''

    def __init__(self):

        self.raw = Raw()
        self.rop = Rop()
        self.inl = Inl()
        self.con = Con()

    def read(self, raw_name, rop_name, inl_name, con_name):

        self.raw.read(raw_name)
        self.rop.read(rop_name)
        self.inl.read(inl_name)
        self.con.read(con_name)

    def write(self, raw_name, rop_name, inl_name, con_name):

        self.raw.write(raw_name)
        self.rop.write(rop_name)
        self.inl.write(inl_name)
        self.con.write(con_name)

    def check(self):
        '''Checks Grid Optimization Competition assumptions'''

        self.raw.check()
        self.rop.check()
        self.inl.check()
        self.con.check()
        self.check_gen_cost_x_margin()
        self.check_no_offline_generators_in_contingencies()
        self.check_no_offline_lines_in_contingencies()
        self.check_no_offline_transformers_in_contingencies()

    def scrub(self):
        '''modifies certain data elements to meet Grid Optimization Competition assumptions'''

        self.raw.switched_shunts_combine_blocks_steps()

    def convert_to_offline(self):
        '''converts the operating point to the offline starting point'''

        self.raw.set_operating_point_to_offline_solution()

    def check_gen_cost_x_margin(self):

        for g in self.raw.get_generators():
            g_i = g.i
            g_id = g.id
            g_pt = g.pt
            g_pb = g.pb
            gdr = self.rop.generator_dispatch_records[(g_i, g_id)]
            apdr = self.rop.active_power_dispatch_records[gdr.dsptbl]
            plcf = self.rop.piecewise_linear_cost_functions[apdr.ctbl]
            plcf.check_x_max_margin(g_pt)
            plcf.check_x_min_margin(g_pb)

    def check_no_offline_generators_in_contingencies(self):
        '''check that no generators that are offline in the base case
        are going out of service in a contingency'''

        gens = self.raw.get_generators()
        offline_gen_keys = [(g.i, g.id) for g in gens if not (g.stat > 0)]
        ctgs = self.con.get_contingencies()
        gen_ctgs = [c for c in ctgs if len(c.generator_out_events) > 0]
        gen_ctg_out_event_map = {
            c: c.generator_out_events[0]
            for c in gen_ctgs}
        gen_ctg_gen_key_ctg_map = {
            (v.i, v.id): k
            for k, v in gen_ctg_out_event_map.items()}
        offline_gens_outaged_in_ctgs_keys = set(offline_gen_keys) & set(gen_ctg_gen_key_ctg_map.keys())
        for g in offline_gens_outaged_in_ctgs_keys:
            gen = self.raw.generators[g]
            ctg = gen_ctg_gen_key_ctg_map[g]
            alert(
                {'data_type':
                     'Data',
                 'error_message':
                     'fails no offline generators going out of service in contingencies. Please ensure that every generator that goes out of service in a contingency is in service in the base case, i.e. has stat=1.',
                 'diagnostics':
                     {'gen i': gen.i,
                      'gen id': gen.id,
                      'gen stat': gen.stat,
                      'ctg label': ctg.label,
                      'ctg gen event i': ctg.generator_out_events[0].i,
                      'ctg gen event id': ctg.generator_out_events[0].id}})

    def check_no_offline_lines_in_contingencies(self):
        '''check that no lines (nontranformer branches) that are offline in the base case
        are going out of service in a contingency'''

        lines = self.raw.get_nontransformer_branches()
        offline_line_keys = [(g.i, g.j, g.ckt) for g in lines if not (g.st > 0)]
        ctgs = self.con.get_contingencies()
        branch_ctgs = [c for c in ctgs if len(c.branch_out_events) > 0]
        branch_ctg_out_event_map = {
            c: c.branch_out_events[0]
            for c in branch_ctgs}
        branch_ctg_branch_key_ctg_map = {
            (v.i, v.j, v.ckt): k
            for k, v in branch_ctg_out_event_map.items()}
        offline_lines_outaged_in_ctgs_keys = set(offline_line_keys) & set(branch_ctg_branch_key_ctg_map.keys())
        for g in offline_lines_outaged_in_ctgs_keys:
            line = self.raw.nontransformer_branches[g]
            ctg = branch_ctg_branch_key_ctg_map[g]
            alert(
                {'data_type':
                     'Data',
                 'error_message':
                     'fails no offline lines going out of service in contingencies. Please ensure that every line (nontransformer branch) that goes out of service in a contingency is in service in the base case, i.e. has st=1.',
                 'diagnostics':
                     {'line i': line.i,
                      'line j': line.j,
                      'line ckt': line.ckt,
                      'line st': line.st,
                      'ctg label': ctg.label,
                      'ctg branch event i': ctg.branch_out_events[0].i,
                      'ctg branch event j': ctg.branch_out_events[0].j,
                      'ctg branch event ckt': ctg.branch_out_events[0].ckt}})

    def check_no_offline_transformers_in_contingencies(self):
        '''check that no branches that are offline in the base case
        are going out of service in a contingency'''

        transformers = self.raw.get_transformers()
        offline_transformer_keys = [(g.i, g.j, g.k) for g in transformers if not (g.stat > 0)]
        ctgs = self.con.get_contingencies()
        branch_ctgs = [c for c in ctgs if len(c.branch_out_events) > 0]
        branch_ctg_out_event_map = {
            c: c.branch_out_events[0]
            for c in branch_ctgs}
        branch_ctg_branch_key_ctg_map = {
            (v.i, v.j, v.ckt): k
            for k, v in branch_ctg_out_event_map.items()}
        offline_transformers_outaged_in_ctgs_keys = set(offline_transformer_keys) & set(branch_ctg_branch_key_ctg_map.keys())
        for g in offline_transformers_outaged_in_ctgs_keys:
            transformer = self.raw.transformers[g]
            ctg = branch_ctg_branch_key_ctg_map[g]
            alert(
                {'data_type':
                     'Data',
                 'error_message':
                     'fails no offline transformers going out of service in contingencies. Please ensure that every transformer that goes out of service in a contingency is in service in the base case, i.e. has stat=1.',
                 'diagnostics':
                     {'transformer i': transformer.i,
                      'transformer j': transformer.j,
                      'transformer ckt': transformer.ckt,
                      'transformer stat': transformer.stat,
                      'ctg label': ctg.label,
                      'ctg branch event i': ctg.branch_out_events[0].i,
                      'ctg branch event j': ctg.branch_out_events[0].j,
                      'ctg branch event ckt': ctg.branch_out_events[0].ckt}})


class Raw:
    '''In physical units, i.e. data convention, i.e. input and output data files'''

    def __init__(self):

        self.case_identification = CaseIdentification()
        self.buses = {}
        self.loads = {}
        self.fixed_shunts = {}
        self.generators = {}
        self.nontransformer_branches = {}
        self.transformers = {}
        self.areas = {}
        self.switched_shunts = {}

    def check(self):

        self.check_case_identification()
        self.check_buses()
        self.check_loads()
        self.check_fixed_shunts()
        self.check_generators()
        self.check_nontransformer_branches()
        self.check_transformers()
        self.check_areas()
        self.check_switched_shunts()

    def check_case_identification(self):

        self.case_identification.check()

    def check_buses(self):

        for r in self.get_buses():
            r.check()

    def check_loads(self):

        for r in self.get_loads():
            r.check()

    def check_fixed_shunts(self):

        for r in self.get_fixed_shunts():
            r.check()

    def check_generators(self):

        for r in self.get_generators():
            r.check()

    def check_nontransformer_branches(self):

        for r in self.get_nontransformer_branches():
            r.check()

    def check_transformers(self):

        for r in self.get_transformers():
            r.check()

    def check_areas(self):

        for r in self.get_areas():
            r.check()

    def check_switched_shunts(self):

        for r in self.get_switched_shunts():
            r.check()

    def set_areas_from_buses(self):

        area_i_set = set([b.area for b in self.buses.values()])

        def area_set_i(area, i):
            area.i = i
            return area

        self.areas = {i: area_set_i(Area(), i) for i in area_i_set}

    def get_buses(self):

        return sorted(self.buses.values(), key=(lambda r: r.i))

    def get_loads(self):

        return sorted(self.loads.values(), key=(lambda r: (r.i, r.id)))

    def get_fixed_shunts(self):

        return sorted(self.fixed_shunts.values(), key=(lambda r: (r.i, r.id)))

    def get_generators(self):

        return sorted(self.generators.values(), key=(lambda r: (r.i, r.id)))

    def get_nontransformer_branches(self):

        return sorted(self.nontransformer_branches.values(), key=(lambda r: (r.i, r.j, r.ckt)))

    def get_transformers(self):

        return sorted(self.transformers.values(), key=(lambda r: (r.i, r.j, r.k, r.ckt)))

    def get_areas(self):

        return sorted(self.areas.values(), key=(lambda r: r.i))

    def get_switched_shunts(self):

        return sorted(self.switched_shunts.values(), key=(lambda r: r.i))

    def construct_case_identification_section(self):

        # out_str = StringIO.StringIO()
        out_str = StringIO()
        # writer = csv.writer(out_str, quotechar="'", quoting=csv.QUOTE_NONNUMERIC)
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        if write_values_in_unused_fields:
            rows = [
                [self.case_identification.ic, self.case_identification.sbase,
                 self.case_identification.rev, self.case_identification.xfrrat,
                 self.case_identification.nxfrat, self.case_identification.basfrq],
                ["%s" % self.case_identification.record_2],  # no quotes here - typical RAW file
                ["%s" % self.case_identification.record_3]]  # no quotes here - typical RAW file
            # ["'%s'" % self.case_identification.record_2],
            # ["'%s'" % self.case_identification.record_3]]
            # ["''"],
            # ["''"]]
        elif write_defaults_in_unused_fields:
            rows = [
                [0, self.case_identification.sbase, 33, 0, 1, 60.0],
                ["''"],
                ["''"]]
        else:
            rows = [
                [None, self.case_identification.sbase, 33, None, None, None],
                ["''"],
                ["''"]]
        writer.writerows(rows)
        return out_str.getvalue()

    def construct_bus_section(self):
        # note use quote_none and quote the strings manually
        # values of None then are written as empty fields, which is what we want

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)

        if write_values_in_unused_fields:
            rows = [
                [r.i, "'%s'" % r.name, r.baskv, r.ide, r.area, r.zone, r.owner, r.vm, r.va, r.nvhi, r.nvlo, r.evhi, r.evlo]
                # for r in self.buses.values()] # might as well sort
                for r in self.get_buses()]
        elif write_defaults_in_unused_fields:
            rows = [
                [r.i, "'            '", 0.0, 1, r.area, 1, 1, r.vm, r.va, r.nvhi, r.nvlo, r.evhi, r.evlo]
                for r in self.get_buses()]
        else:
            rows = [
                [r.i, None, None, None, r.area, None, None, r.vm, r.va, r.nvhi, r.nvlo, r.evhi, r.evlo]
                for r in self.get_buses()]
        writer.writerows(rows)
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF BUS DATA BEGIN LOAD DATA']])  # no comma allowed without escape character
        # out_str.write('0 / END OF BUS DATA, BEGIN LOAD DATA\n')
        return out_str.getvalue()

    def construct_load_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        if write_values_in_unused_fields:
            rows = [
                [r.i, "'%s'" % r.id, r.status, r.area, r.zone, r.pl, r.ql, r.ip, r.iq, r.yp, r.yq, r.owner, r.scale, r.intrpt]
                for r in self.get_loads()]
        elif write_defaults_in_unused_fields:
            rows = [
                [r.i, "'%s'" % r.id, r.status, self.buses[r.i].area, 1, r.pl, r.ql, 0.0, 0.0, 0.0, 0.0, 1, 1, 0]
                for r in self.get_loads()]
        else:
            rows = [
                [r.i, "'%s'" % r.id, r.status, None, None, r.pl, r.ql, None, None, None, None, None, None, None]
                for r in self.get_loads()]
        writer.writerows(rows)
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF LOAD DATA BEGIN FIXED SHUNT DATA']])
        return out_str.getvalue()

    def construct_fixed_shunt_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        if write_values_in_unused_fields:
            rows = [
                [r.i, "'%s'" % r.id, r.status, r.gl, r.bl]
                for r in self.get_fixed_shunts()]
        elif write_defaults_in_unused_fields:
            rows = [
                [r.i, "'%s'" % r.id, r.status, r.gl, r.bl]
                for r in self.get_fixed_shunts()]
        else:
            rows = [
                [r.i, "'%s'" % r.id, r.status, r.gl, r.bl]
                for r in self.get_fixed_shunts()]
        writer.writerows(rows)
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF FIXED SHUNT DATA BEGIN GENERATOR DATA']])
        return out_str.getvalue()

    def construct_generator_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        if write_values_in_unused_fields:
            rows = [
                [r.i, "'%s'" % r.id, r.pg, r.qg, r.qt, r.qb,
                 r.vs, r.ireg, r.mbase, r.zr, r.zx, r.rt, r.xt, r.gtap,
                 r.stat, r.rmpct, r.pt, r.pb, r.o1, r.f1, r.o2,
                 r.f2, r.o3, r.f3, r.o4, r.f4, r.wmod, r.wpf]
                for r in self.get_generators()]
        elif write_defaults_in_unused_fields:
            rows = [
                [r.i, "'%s'" % r.id, r.pg, r.qg, r.qt, r.qb,
                 1.0, 0, self.case_identification.sbase, 0.0, 1.0, 0.0, 0.0, 1.0,
                 r.stat, 100.0, r.pt, r.pb, 1, 1.0, 0,
                 1.0, 0, 1.0, 0, 1.0, 0, 1.0]
                for r in self.get_generators()]
        else:
            rows = [
                [r.i, "'%s'" % r.id, r.pg, r.qg, r.qt, r.qb,
                 None, None, None, None, None, None, None, None,
                 r.stat, None, r.pt, r.pb, None, None, None,
                 None, None, None, None, None, None, None]
                for r in self.get_generators()]
        writer.writerows(rows)
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF GENERATOR DATA BEGIN BRANCH DATA']])
        return out_str.getvalue()

    def construct_nontransformer_branch_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        if write_values_in_unused_fields:
            rows = [
                [r.i, r.j, "'%s'" % r.ckt, r.r, r.x, r.b, r.ratea,
                 r.rateb, r.ratec, r.gi, r.bi, r.gj, r.bj, r.st, r.met, r.len,
                 r.o1, r.f1, r.o2, r.f2, r.o3, r.f3, r.o4, r.f4]
                for r in self.get_nontransformer_branches()]
        elif write_defaults_in_unused_fields:
            rows = [
                [r.i, r.j, "'%s'" % r.ckt, r.r, r.x, r.b, r.ratea,
                 0.0, r.ratec, 0.0, 0.0, 0.0, 0.0, r.st, 1, 0.0,
                 1, 1.0, 0, 1.0, 0, 1.0, 0, 1.0]
                for r in self.get_nontransformer_branches()]
        else:
            rows = [
                [r.i, r.j, "'%s'" % r.ckt, r.r, r.x, r.b, r.ratea,
                 None, r.ratec, None, None, None, None, r.st, None, None,
                 None, None, None, None, None, None, None, None]
                for r in self.get_nontransformer_branches()]
        writer.writerows(rows)
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF BRANCH DATA BEGIN TRANSFORMER DATA']])
        return out_str.getvalue()

    def construct_transformer_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        if write_values_in_unused_fields:
            rows = [
                rr
                for r in self.get_transformers()
                for rr in [
                    [r.i, r.j, r.k, "'%s'" % r.ckt, r.cw, r.cz, r.cm,
                     r.mag1, r.mag2, r.nmetr, "'%s'" % r.name, r.stat, r.o1, r.f1,
                     r.o2, r.f2, r.o3, r.f3, r.o4, r.f4, "'%s'" % r.vecgrp],
                    [r.r12, r.x12, r.sbase12],
                    [r.windv1, r.nomv1, r.ang1, r.rata1, r.ratb1, r.ratc1,
                     r.cod1, r.cont1, r.rma1, r.rmi1, r.vma1, r.vmi1, r.ntp1, r.tab1,
                     r.cr1, r.cx1, r.cnxa1],
                    [r.windv2, r.nomv2]]]
        elif write_defaults_in_unused_fields:
            rows = [
                rr
                for r in self.get_transformers()
                for rr in [
                    [r.i, r.j, 0, "'%s'" % r.ckt, 1, 1, 1,
                     r.mag1, r.mag2, 2, "'            '", r.stat, 1, 1.0,
                     0, 1.0, 0, 1.0, 0, 1.0, "'            '"],
                    [r.r12, r.x12, self.case_identification.sbase],
                    [r.windv1, 0.0, r.ang1, r.rata1, 0.0, r.ratc1,
                     0, 0, 1.1, 0.9, 1.1, 0.9, 33, 0,
                     0.0, 0.0, 0.0],
                    [r.windv2, 0.0]]]
        else:
            rows = [
                rr
                for r in self.get_transformers()
                for rr in [
                    [r.i, r.j, 0, "'%s'" % r.ckt, None, None, None,
                     r.mag1, r.mag2, None, None, r.stat, None, None,
                     None, None, None, None, None, None, None],
                    [r.r12, r.x12, None],
                    [r.windv1, None, r.ang1, r.rata1, None, r.ratc1,
                     None, None, None, None, None, None, None, None,
                     None, None, None],
                    [r.windv2, None]]]
        writer.writerows(rows)
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF TRANSFORMER DATA BEGIN AREA DATA']])
        return out_str.getvalue()

    def construct_area_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        if write_values_in_unused_fields:
            rows = [
                [r.i, r.isw, r.pdes, r.ptol, "'%s'" % r.arname]
                for r in self.get_areas()]
        elif write_defaults_in_unused_fields:
            rows = [
                [r.i, 0, 0.0, 10.0, "'            '"]
                for r in self.get_areas()]
        else:
            rows = [
                [r.i, None, None, None, None]
                for r in self.get_areas()]
        writer.writerows(rows)
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF AREA DATA BEGIN TWO-TERMINAL DC DATA']])
        return out_str.getvalue()

    def construct_two_terminal_dc_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF TWO-TERMINAL DC DATA BEGIN VSC DC LINE DATA']])
        return out_str.getvalue()

    def construct_vsc_dc_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF VSC DC LINE DATA BEGIN IMPEDANCE CORRECTION DATA']])
        return out_str.getvalue()

    def construct_transformer_impedance_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF IMPEDANCE CORRECTION DATA BEGIN MULTI-TERMINAL DC DATA']])
        return out_str.getvalue()

    def construct_multi_terminal_dc_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF MULTI-TERMINAL DC DATA BEGIN MULTI-SECTION LINE DATA']])
        return out_str.getvalue()

    def construct_multi_section_line_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF MULTI-SECTION LINE DATA BEGIN ZONE DATA']])
        return out_str.getvalue()

    def construct_zone_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF ZONE DATA BEGIN INTER-AREA TRANSFER DATA']])
        return out_str.getvalue()

    def construct_interarea_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF INTER-AREA TRANSFER DATA BEGIN OWNER DATA']])
        return out_str.getvalue()

    def construct_owner_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF OWNER DATA BEGIN FACTS DEVICE DATA']])
        return out_str.getvalue()

    def construct_facts_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF FACTS DEVICE DATA BEGIN SWITCHED SHUNT DATA']])
        return out_str.getvalue()

    def construct_switched_shunt_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        if write_values_in_unused_fields:
            rows = [
                [r.i, r.modsw, r.adjm, r.stat, r.vswhi, r.vswlo, r.swrem, r.rmpct, "'%s'" % r.rmidnt,
                 r.binit, r.n1, r.b1, r.n2, r.b2, r.n3, r.b3, r.n4, r.b4,
                 r.n5, r.b5, r.n6, r.b6, r.n7, r.b7, r.n8, r.b8]
                for r in self.get_switched_shunts()]
        elif write_defaults_in_unused_fields:
            rows = [
                [r.i, 1, 0, r.stat, 1.0, 1.0, 0, 100.0, "'            '",
                 r.binit, r.n1, r.b1, r.n2, r.b2, r.n3, r.b3, r.n4, r.b4,
                 r.n5, r.b5, r.n6, r.b6, r.n7, r.b7, r.n8, r.b8]
                for r in self.get_switched_shunts()]
        else:
            rows = [
                [r.i, None, None, r.stat, None, None, None, None, None,
                 r.binit, r.n1, r.b1, r.n2, r.b2, r.n3, r.b3, r.n4, r.b4,
                 r.n5, r.b5, r.n6, r.b6, r.n7, r.b7, r.n8, r.b8]
                for r in self.get_switched_shunts()]
        writer.writerows(rows)
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF SWITCHED SHUNT DATA BEGIN GNE DATA']])
        return out_str.getvalue()

    def construct_gne_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF GNE DATA BEGIN INDUCTION MACHINE DATA']])
        return out_str.getvalue()

    def construct_induction_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF INDUCTION MACHINE DATA']])
        return out_str.getvalue()

    def construct_q_record(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        rows = [['Q']]
        writer.writerows(rows)
        return out_str.getvalue()

    def write(self, file_name):
        '''write a RAW file'''

        with open(file_name, 'w') as out_file:
            out_file.write(self.construct_case_identification_section())
            out_file.write(self.construct_bus_section())
            out_file.write(self.construct_load_section())
            out_file.write(self.construct_fixed_shunt_section())
            out_file.write(self.construct_generator_section())
            out_file.write(self.construct_nontransformer_branch_section())
            out_file.write(self.construct_transformer_section())
            out_file.write(self.construct_area_section())
            out_file.write(self.construct_two_terminal_dc_section())
            out_file.write(self.construct_vsc_dc_section())
            out_file.write(self.construct_transformer_impedance_section())
            out_file.write(self.construct_multi_terminal_dc_section())
            out_file.write(self.construct_multi_section_line_section())
            out_file.write(self.construct_zone_section())
            out_file.write(self.construct_interarea_section())
            out_file.write(self.construct_owner_section())
            out_file.write(self.construct_facts_section())
            out_file.write(self.construct_switched_shunt_section())
            out_file.write(self.construct_gne_section())
            out_file.write(self.construct_induction_section())
            out_file.write(self.construct_q_record())

    def switched_shunts_combine_blocks_steps(self):

        for r in self.switched_shunts.values():
            b_min = 0.0
            b_max = 0.0
            b1 = float(r.n1) * r.b1
            b2 = float(r.n2) * r.b2
            b3 = float(r.n3) * r.b3
            b4 = float(r.n4) * r.b4
            b5 = float(r.n5) * r.b5
            b6 = float(r.n6) * r.b6
            b7 = float(r.n7) * r.b7
            b8 = float(r.n8) * r.b8
            for b in [b1, b2, b3, b4, b5, b6, b7, b8]:
                if b > 0.0:
                    b_max += b
                elif b < 0.0:
                    b_min += b
                else:
                    break
            r.n1 = 0
            r.b1 = 0.0
            r.n2 = 0
            r.b2 = 0.0
            r.n3 = 0
            r.b3 = 0.0
            r.n4 = 0
            r.b4 = 0.0
            r.n5 = 0
            r.b5 = 0.0
            r.n6 = 0
            r.b6 = 0.0
            r.n7 = 0
            r.b7 = 0.0
            r.n8 = 0
            r.b8 = 0.0
            if b_max > 0.0:
                r.n1 = 1
                r.b1 = b_max
                if b_min < 0.0:
                    r.n2 = 1
                    r.b2 = b_min
            elif b_min < 0.0:
                r.n1 = 1
                r.b1 = b_min

    def set_operating_point_to_offline_solution(self):

        for r in self.buses.values():
            r.vm = 1.0
            r.va = 0.0
        for r in self.generators.values():
            r.pg = 0.0
            r.qg = 0.0
        for r in self.switched_shunts.values():
            r.binit = 0.0

    def read(self, file_name):

        with open(file_name, 'r') as in_file:
            lines = in_file.readlines()
        delimiter_str = ","
        quote_str = "'"
        skip_initial_space = True
        rows = csv.reader(
            lines,
            delimiter=delimiter_str,
            quotechar=quote_str,
            skipinitialspace=skip_initial_space)
        rows = [[t.strip() for t in r] for r in rows]
        self.read_from_rows(rows)
        self.set_areas_from_buses()

    def row_is_file_end(self, row):

        is_file_end = False
        if len(row) == 0:
            is_file_end = True
        if row[0][:1] in {'', 'q', 'Q'}:
            is_file_end = True
        return is_file_end

    def row_is_section_end(self, row):

        is_section_end = False
        if row[0][:1] == '0':
            is_section_end = True
        return is_section_end

    def read_from_rows(self, rows):

        row_num = 0
        cid_rows = rows[row_num:(row_num + 3)]
        self.case_identification.read_from_rows(rows)
        row_num += 2
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
            bus = Bus()
            bus.read_from_row(row)
            self.buses[bus.i] = bus
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
            load = Load()
            load.read_from_row(row)
            self.loads[(load.i, load.id)] = load
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
            fixed_shunt = FixedShunt()
            fixed_shunt.read_from_row(row)
            self.fixed_shunts[(fixed_shunt.i, fixed_shunt.id)] = fixed_shunt
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
            generator = Generator()
            generator.read_from_row(row)
            self.generators[(generator.i, generator.id)] = generator
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
            nontransformer_branch = NontransformerBranch()
            nontransformer_branch.read_from_row(row)
            self.nontransformer_branches[(
                nontransformer_branch.i,
                nontransformer_branch.j,
                nontransformer_branch.ckt)] = nontransformer_branch
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
            transformer = Transformer()
            num_rows = transformer.get_num_rows_from_row(row)
            rows_temp = rows[
                        row_num:(row_num + num_rows)]
            transformer.read_from_rows(rows_temp)
            self.transformers[(
                transformer.i,
                transformer.j,
                # transformer.k,
                0,
                transformer.ckt)] = transformer
            row_num += (num_rows - 1)
        while True:  # areas - for now just make a set of areas based on bus info
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
            area = Area()
            area.read_from_row(row)
            self.areas[area.i] = area
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
        while True:  # zone
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
            switched_shunt = SwitchedShunt()
            switched_shunt.read_from_row(row)
            self.switched_shunts[switched_shunt.i] = switched_shunt


class Rop:
    '''In physical units, i.e. data convention, i.e. input and output data files'''

    def __init__(self):

        self.generator_dispatch_records = {}
        self.active_power_dispatch_records = {}
        self.piecewise_linear_cost_functions = {}

    def check(self):

        self.check_generator_dispatch_records()
        self.check_active_power_dispatch_records()
        self.check_piecewise_linear_cost_functions()

    def check_generator_dispatch_records(self):

        for r in self.get_generator_dispatch_records():
            r.check()

    def check_active_power_dispatch_records(self):

        for r in self.get_active_power_dispatch_records():
            r.check()

    def check_piecewise_linear_cost_functions(self):

        for r in self.get_piecewise_linear_cost_functions():
            r.check()

    def trancostfuncfrom_phase_0(self, rawdata):
        ds = self.active_power_dispatch_records.get((4, '1'))

        for r in rawdata.generators.values():
            ds = self.active_power_dispatch_records.get((r.i, r.id))
            # update piecewise linear info
            self.active_power_dispatch_records.get((r.i, r.id)).npairs = 10
            self.active_power_dispatch_records.get((r.i, r.id)).costzero = self.active_power_dispatch_records.get((r.i, r.id)).constc
            for i in range(self.active_power_dispatch_records.get((r.i, r.id)).npairs):
                # the points will be power followed by cost, ie[power0 cost0 power 1 cost1 ....]
                self.active_power_dispatch_records.get((r.i, r.id)).points.append(r.pb + i * (r.pt - r.pb) / (self.active_power_dispatch_records.get((r.i, r.id)).npairs - 1))
                self.active_power_dispatch_records.get((r.i, r.id)).points.append(
                    self.active_power_dispatch_records.get((r.i, r.id)).constc + self.active_power_dispatch_records.get((r.i, r.id)).linearc * (
                                r.pb + i * (r.pt - r.pb) / (self.active_power_dispatch_records.get((r.i, r.id)).npairs - 1)) + self.active_power_dispatch_records.get(
                        (r.i, r.id)).quadraticc * pow(r.pb + i * (r.pt - r.pb) / (self.active_power_dispatch_records.get((r.i, r.id)).npairs - 1), 2))

    def read_from_phase_0(self, file_name):

        '''takes the generator.csv file as input'''
        with open(file_name, 'r') as in_file:
            lines = in_file.readlines()
        del lines[0]
        delimiter_str = ","
        quote_str = "'"
        skip_initial_space = True
        rows = csv.reader(
            lines,
            delimiter=delimiter_str,
            quotechar=quote_str,
            skipinitialspace=skip_initial_space)
        # [[t.strip() for t in r] for r in rows]
        for r in rows:
            indi = 1
            gen_dispatch = QuadraticCostFunctions()
            gen_dispatch.read_from_csv(r)

            gen_dispatch.read_from_csv_quadraticinfo(r)
            while (indi < 4):
                r2 = rows.next()
                gen_dispatch.read_from_csv_quadraticinfo(r2)
                indi = indi + 1
                # print([gen_dispatch.bus,gen_dispatch.genid, gen_dispatch.constc,gen_dispatch.linearc,gen_dispatch.quadraticc])
            self.active_power_dispatch_records[gen_dispatch.bus, gen_dispatch.genid] = gen_dispatch
        # print([gen_dispatch.bus,gen_dispatch.genid, gen_dispatch.constc])
        # ds=self.active_power_dispatch_records.get((4, '1'))

    def get_generator_dispatch_records(self):

        return sorted(self.generator_dispatch_records.values(), key=(lambda r: (r.bus, r.genid)))

    def get_active_power_dispatch_records(self):

        return sorted(self.active_power_dispatch_records.values(), key=(lambda r: r.tbl))

    def get_piecewise_linear_cost_functions(self):

        return sorted(self.piecewise_linear_cost_functions.values(), key=(lambda r: r.ltbl))

    def construct_data_modification_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF MODIFICATION CODE BEGIN BUS VOLTAGE CONSTRAINT DATA']])
        return out_str.getvalue()

    def construct_bus_voltage_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF BUS VOLTAGE CONSTRAINT DATA BEGIN ADJUSTABLE BUS SHUNT DATA']])
        return out_str.getvalue()

    def construct_adjustable_bus_shunt_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF ADJUSTABLE BUS SHUNT DATA BEGIN BUS LOAD DATA']])
        return out_str.getvalue()

    def construct_bus_load_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF BUS LOAD DATA BEGIN ADJUSTABLE BUS LOAD TABLES']])
        return out_str.getvalue()

    def construct_adjustable_bus_load_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF ADJUSTABLE BUS LOAD TABLES BEGIN GENERATOR DISPATCH DATA']])
        return out_str.getvalue()

    def construct_generator_dispatch_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        if write_values_in_unused_fields:
            rows = [
                [r.bus, r.genid, r.disp, r.dsptbl]
                for r in self.get_generator_dispatch_records()]
        elif write_defaults_in_unused_fields:
            rows = [
                [r.bus, r.genid, 1.0, r.dsptbl]
                for r in self.get_generator_dispatch_records()]
        else:
            rows = [
                [r.bus, r.genid, None, r.dsptbl]
                for r in self.get_generator_dispatch_records()]
        writer.writerows(rows)
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF GENERATOR DISPATCH DATA BEGIN ACTIVE POWER DISPATCH TABLES']])
        return out_str.getvalue()

    def construct_active_power_dispatch_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        if write_values_in_unused_fields:
            rows = [
                [r.tbl, r.pmax, r.pmin, r.fuelcost, r.ctyp, r.status, r.ctbl]
                for r in self.get_active_power_dispatch_records()]
        elif write_defaults_in_unused_fields:
            rows = [
                [r.tbl, 9999.0, -9999.0, 1.0, 1, 1, r.ctbl]
                for r in self.get_active_power_dispatch_records()]
        else:
            rows = [
                [r.tbl, None, None, None, None, None, r.ctbl]
                for r in self.get_active_power_dispatch_records()]
        writer.writerows(rows)
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF ACTIVE POWER DISPATCH TABLES BEGIN GENERATION RESERVE DATA']])
        return out_str.getvalue()

    def construct_generator_reserve_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF GENERATION RESERVE DATA BEGIN GENERATION REACTIVE CAPABILITY DATA']])
        return out_str.getvalue()

    def construct_reactive_capability_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF GENERATION REACTIVE CAPABILITY DATA BEGIN ADJUSTABLE BRANCH REACTANCE DATA']])
        return out_str.getvalue()

    def construct_branch_reactance_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF ADJUSTABLE BRANCH REACTANCE DATA BEGIN PIECE-WISE LINEAR COST TABLES']])
        return out_str.getvalue()

    def construct_piecewise_linear_cost_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        if write_values_in_unused_fields:
            rows = [
                row
                for r in self.get_piecewise_linear_cost_functions()
                for row in (
                        [[r.ltbl, "'%s'" % r.label, r.npairs]] +
                        [[p.x, p.y] for p in r.points])]
        elif write_defaults_in_unused_fields:
            rows = [
                row
                for r in self.get_piecewise_linear_cost_functions()
                for row in (
                        [[r.ltbl, "''", r.npairs]] +
                        [[p.x, p.y] for p in r.points])]
        else:
            rows = [
                row
                for r in self.get_piecewise_linear_cost_functions()
                for row in (
                        [[r.ltbl, None, r.npairs]] +
                        [[p.x, p.y] for p in r.points])]
        writer.writerows(rows)
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF PIECE-WISE LINEAR COST TABLES BEGIN PIECEWISE QUADRATIC COST TABLES']])
        return out_str.getvalue()

    def construct_piecewise_quadratic_cost_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF PIECE-WISE QUADRATIC COST TABLES BEGIN POLYNOMIAL AND EXPONENTIAL COST TABLES']])
        return out_str.getvalue()

    def construct_polynomial_exponential_cost_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF POLYNOMIAL COST TABLES BEGIN PERIOD RESERVE DATA']])
        return out_str.getvalue()

    def construct_period_reserve_constraint_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF PERIOD RESERVE DATA BEGIN BRANCH FLOW CONSTRAINT DATA']])
        return out_str.getvalue()

    def construct_branch_flow_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF BRANCH FLOW CONSTRAINT DATA BEGIN INTERFACE FLOW DATA']])
        return out_str.getvalue()

    def construct_interface_flow_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF INTERFACE FLOW DATA BEGIN LINEAR CONSTRAINT EQUATION DEPENDENCY DATA']])
        return out_str.getvalue()

    def construct_linear_constraint_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF LINEAR CONSTRAINT EQUATION DEPENDENCY DATA']])
        return out_str.getvalue()
        # 0 / End of Linear Constraint Equation Dependency data, begin 2-terminal dc Line Constraint data
        # 0 / End of 2-terminal dc Line Constraint data

    def construct_q_record(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['Q']])
        return out_str.getvalue()

    def write(self, file_name):
        '''write an ROP file'''

        with open(file_name, 'w') as out_file:
            out_file.write(self.construct_data_modification_section())
            out_file.write(self.construct_bus_voltage_section())
            out_file.write(self.construct_adjustable_bus_shunt_section())
            out_file.write(self.construct_bus_load_section())
            out_file.write(self.construct_adjustable_bus_load_section())
            out_file.write(self.construct_generator_dispatch_section())
            out_file.write(self.construct_active_power_dispatch_section())
            out_file.write(self.construct_generator_reserve_section())
            out_file.write(self.construct_reactive_capability_section())
            out_file.write(self.construct_branch_reactance_section())
            out_file.write(self.construct_piecewise_linear_cost_section())
            out_file.write(self.construct_piecewise_quadratic_cost_section())
            out_file.write(self.construct_polynomial_exponential_cost_section())
            out_file.write(self.construct_period_reserve_constraint_section())
            out_file.write(self.construct_branch_flow_section())
            out_file.write(self.construct_interface_flow_section())
            out_file.write(self.construct_linear_constraint_section())
            out_file.write(self.construct_q_record())  # no q record in sample ROP file

    def read(self, file_name):

        with open(file_name, 'r') as in_file:
            lines = in_file.readlines()
        delimiter_str = ","
        quote_str = "'"
        skip_initial_space = True
        rows = csv.reader(
            lines,
            delimiter=delimiter_str,
            quotechar=quote_str,
            skipinitialspace=skip_initial_space)
        rows = [[t.strip() for t in r] for r in rows]
        self.read_from_rows(rows)

    def row_is_file_end(self, row):

        is_file_end = False
        if len(row) == 0:
            is_file_end = True
        if row[0][:1] in {'', 'q', 'Q'}:
            is_file_end = True
        return is_file_end

    def row_is_section_end(self, row):

        is_section_end = False
        if row[0][:1] == '0':
            is_section_end = True
        return is_section_end

    def read_from_rows(self, rows):

        row_num = -1
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
            generator_dispatch_record = GeneratorDispatchRecord()
            generator_dispatch_record.read_from_row(row)
            self.generator_dispatch_records[(
                generator_dispatch_record.bus,
                generator_dispatch_record.genid)] = generator_dispatch_record
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
            active_power_dispatch_record = ActivePowerDispatchRecord()
            active_power_dispatch_record.read_from_row(row)
            self.active_power_dispatch_records[
                active_power_dispatch_record.tbl] = (
                active_power_dispatch_record)
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
            piecewise_linear_cost_function = PiecewiseLinearCostFunction()
            num_rows = piecewise_linear_cost_function.get_num_rows_from_row(row)
            rows_temp = rows[
                        row_num:(row_num + num_rows)]
            piecewise_linear_cost_function.read_from_rows(rows_temp)
            self.piecewise_linear_cost_functions[
                piecewise_linear_cost_function.ltbl] = (
                piecewise_linear_cost_function)
            row_num += (num_rows - 1)


class Inl:
    '''In physical units, i.e. data convention, i.e. input and output data files'''

    def __init__(self):

        self.generator_inl_records = {}

    def check(self):

        for r in self.get_generator_inl_records():
            r.check()

    # TODO
    def read_from_phase_0(self, file_name):
        '''takes the generator.csv file as input'''

    def write(self, file_name):
        '''write an INL file'''

        with open(file_name, 'w') as out_file:
            out_file.write(self.construct_data_section())

    def get_generator_inl_records(self):

        return sorted(self.generator_inl_records.values(), key=(lambda r: (r.i, r.id)))

    def construct_data_section(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        if write_values_in_unused_fields:
            rows = [
                [r.i, r.id, r.h, r.pmax, r.pmin, r.r, r.d]
                for r in self.get_generator_inl_records()]
        elif write_defaults_in_unused_fields:
            rows = [
                [r.i, r.id, 4.0, 0.0, 0.0, r.r, 0.0]
                for r in self.get_generator_inl_records()]
        else:
            rows = [
                [r.i, r.id, "", "", "", r.r, ""]
                for r in self.get_generator_inl_records()]
        writer.writerows(rows)
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE)
        writer.writerows([['0 / END OF DATA']])
        return out_str.getvalue()

    def read(self, file_name):

        with open(file_name, 'r') as in_file:
            lines = in_file.readlines()
        delimiter_str = ","
        quote_str = "'"
        skip_initial_space = True
        rows = csv.reader(
            lines,
            delimiter=delimiter_str,
            quotechar=quote_str,
            skipinitialspace=skip_initial_space)
        rows = [[t.strip() for t in r] for r in rows]
        self.read_from_rows(rows)

    def row_is_file_end(self, row):

        is_file_end = False
        if len(row) == 0:
            is_file_end = True
        if row[0][:1] in {'', 'q', 'Q'}:
            is_file_end = True
        return is_file_end

    def row_is_section_end(self, row):

        is_section_end = False
        if row[0][:1] == '0':
            is_section_end = True
        return is_section_end

    def read_from_rows(self, rows):

        row_num = -1
        while True:
            row_num += 1
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            if self.row_is_section_end(row):
                break
            generator_inl_record = GeneratorInlRecord()
            generator_inl_record.read_from_row(row)
            self.generator_inl_records[(
                generator_inl_record.i,
                generator_inl_record.id)] = generator_inl_record


class Con:
    '''In physical units, i.e. data convention, i.e. input and output data files'''

    def __init__(self):

        self.contingencies = {}

    def check(self):

        for r in self.get_contingencies():
            r.check()

    def read_from_phase_0(self, file_name):
        '''takes the contingency.csv file as input'''
        with open(file_name, 'r') as in_file:
            lines = in_file.readlines()
        delimiter_str = " "
        quote_str = "'"
        skip_initial_space = True
        del lines[0]
        rows = csv.reader(
            lines,
            delimiter=delimiter_str,
            quotechar=quote_str,
            skipinitialspace=skip_initial_space)
        rows = [[t.strip() for t in r] for r in rows]
        quote_str = "'"
        contingency = Contingency()
        # there is no contingency label for continency.csv
        for r in rows:
            tmprow = r[0].split(',')
            if tmprow[1].upper() == 'B' or tmprow[1].upper() == 'T':
                contingency.label = "LINE-" + tmprow[2] + "-" + tmprow[3] + "-" + tmprow[4]
                branch_out_event = BranchOutEvent()
                branch_out_event.read_from_csv(tmprow)
                contingency.branch_out_events.append(branch_out_event)
                self.contingencies[contingency.label] = branch_out_event
            elif tmprow[1].upper() == 'G':
                contingency.label = "GEN-" + tmprow[2] + "-" + tmprow[3]
                generator_out_event = GeneratorOutEvent()
                generator_out_event.read_from_csv(tmprow)
                contingency.generator_out_events.append(generator_out_event)
                self.contingency.generator_out_event.read_from_csv(tmprow)

    def get_contingencies(self):

        return sorted(self.contingencies.values(), key=(lambda r: r.label))

    def construct_data_records(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE, delimiter=' ')
        rows = [
            row
            for r in self.get_contingencies()
            for row in r.construct_record_rows()]
        writer.writerows(rows)
        return out_str.getvalue()

    def construct_end_record(self):

        out_str = StringIO()
        writer = csv.writer(out_str, quoting=csv.QUOTE_NONE, delimiter=' ')
        rows = [['END']]
        writer.writerows(rows)
        return out_str.getvalue()

    def write(self, file_name):
        '''write a CON file'''

        with open(file_name, 'w') as out_file:
            out_file.write(self.construct_data_records())
            out_file.write(self.construct_end_record())

    def read(self, file_name):

        with open(file_name, 'r') as in_file:
            lines = in_file.readlines()
        try:
            for l in lines:
                if l.find("'") > -1 or l.find('"') > -1:
                    print('no quotes allowed, line:')
                    print(l)
                    alert(
                        {'data_type': 'Con',
                         'error_message': 'no quotes allowed in CON file',
                         'diagnostics': l})
                    if raise_con_quote:
                        raise Exception('no quotes allowed in CON')
        except Exception as e:
            traceback.print_exc()
            raise e
        delimiter_str = " "
        # quote_str = "'"
        skip_initial_space = True
        rows = csv.reader(
            lines,
            delimiter=delimiter_str,
            # quotechar=quote_str,
            skipinitialspace=skip_initial_space,
            quoting=csv.QUOTE_NONE)  # QUOTE_NONE
        rows = [[t.strip() for t in r] for r in rows]
        self.read_from_rows(rows)

    def row_is_file_end(self, row):

        is_file_end = False
        if len(row) == 0:
            is_file_end = True
        if row[0][:1] in {'', 'q', 'Q'}:
            is_file_end = True
        return is_file_end

    # def row_is_section_end(self, row):
    #
    #    is_section_end = False
    #    if row[0][:1] == '0':
    #        is_section_end = True
    #    return is_section_end

    def is_contingency_start(self, row):

        return (row[0].upper() == 'CONTINGENCY')

    def is_end(self, row):

        return (row[0].upper() == 'END')

    def is_branch_out_event(self, row):

        # return (
        #    row[0].upper() in {'DISCONNECT', 'OPEN', 'TRIP'} and
        #    row[1].upper() in {'BRANCH', 'LINE'})
        return (row[0] == 'OPEN' and row[1] == 'BRANCH')

    def is_three_winding(self, row):

        # print(row)
        if len(row) < 9:
            return False
        elif row[8].upper() == 'TO':
            return True
        else:
            return False

    def is_generator_out_event(self, row):

        # return(
        #    row[0].upper() == 'REMOVE' and
        #    row[1].upper() in {'UNIT', 'MACHINE'})
        return (row[0] == 'REMOVE' and row[1] == 'UNIT')

    def read_from_rows(self, rows):

        row_num = -1
        in_contingency = False
        while True:
            row_num += 1
            # if row_num >= len(rows): # in case the data provider failed to put an end file line
            #    return
            row = rows[row_num]
            if self.row_is_file_end(row):
                return
            # if self.row_is_section_end(row):
            #    break
            elif self.is_contingency_start(row):
                in_contingency = True
                contingency = Contingency()
                contingency.label = row[1]
            elif self.is_end(row):
                if in_contingency:
                    self.contingencies[contingency.label] = contingency
                    in_contingency = False
                else:
                    break
            elif self.is_branch_out_event(row):
                branch_out_event = BranchOutEvent()
                if self.is_three_winding(row):
                    branch_out_event.read_three_winding_from_row(row)
                else:
                    branch_out_event.read_from_row(row)
                contingency.branch_out_events.append(branch_out_event)
            elif self.is_generator_out_event(row):
                generator_out_event = GeneratorOutEvent()
                generator_out_event.read_from_row(row)
                contingency.generator_out_events.append(generator_out_event)
            else:
                try:
                    print('format error in CON file row:')
                    print(row)
                    raise Exception('format error in CON file')
                except Exception as e:
                    traceback.print_exc()
                    raise e


class CaseIdentification:

    def __init__(self):

        self.ic = 0
        self.sbase = 100.0
        self.rev = 33
        self.xfrrat = 0
        self.nxfrat = 1
        self.basfrq = 60.0
        self.record_2 = 'GRID OPTIMIZATION COMPETITION'
        self.record_3 = 'INPUT DATA FILES ARE RAW ROP INL CON'

    def check(self):

        self.check_sbase_positive()

    def check_sbase_positive(self):

        if not (self.sbase > 0.0):
            alert(
                {'data_type':
                     'CaseIdentification',
                 'error_message':
                     'fails sbase positivitiy. please ensure that sbase > 0.0',
                 'diagnostics':
                     {'sbase': self.sbase}})

    def read_record_1_from_row(self, row):

        row = pad_row(row, 6)
        # row[5] = remove_end_of_line_comment(row[5], '/')
        self.sbase = parse_token(row[1], float, default=None)
        if read_unused_fields:
            self.ic = parse_token(row[0], int, 0)
            self.rev = parse_token(row[2], int, 33)
            self.xfrrat = parse_token(row[3], int, 0)
            self.nxfrat = parse_token(row[4], int, 1)
            self.basfrq = parse_token(row[5], float, 60.0)  # need to remove end of line comment

    def read_from_rows(self, rows):

        self.read_record_1_from_row(rows[0])
        # self.record_2 = '' # not preserving these at this point
        # self.record_3 = '' # do that later


class Bus:

    def __init__(self):

        self.i = None  # no default allowed - we want this to throw an error
        self.name = 12 * ' '
        self.baskv = 0.0
        self.ide = 1
        self.area = 1
        self.zone = 1
        self.owner = 1
        self.vm = 1.0
        self.va = 0.0
        self.nvhi = 1.1
        self.nvlo = 0.9
        self.evhi = 1.1
        self.evlo = 0.9

    def check(self):

        self.check_i_pos()
        self.check_area_pos()
        self.check_vm_pos()
        self.check_nvhi_pos()
        self.check_nvlo_pos()
        self.check_evhi_pos()
        self.check_evlo_pos()
        self.check_nvhi_nvlo_consistent()
        self.check_evhi_evlo_consistent()
        self.check_evhi_nvhi_consistent()
        self.check_nvlo_evlo_consistent()
        # check vm within bounds?
        # check area in areas?

    def clean_name(self):

        self.name = ''

    def check_i_pos(self):

        if not (self.i > 0):
            alert(
                {'data_type': 'Bus',
                 'error_message': 'fails i positivity. Please ensure that the i field of every bus is a positive integer',
                 'diagnostics': {
                     'i': self.i}})

    def check_area_pos(self):

        if not (self.area > 0):
            alert(
                {'data_type': 'Bus',
                 'error_message': 'fails area positivity. Please ensure that the area field of every bus is a positive integer',
                 'diagnostics': {
                     'i': self.i,
                     'area': self.area}})

    def check_vm_pos(self):

        if not (self.vm > 0.0):
            alert(
                {'data_type': 'Bus',
                 'error_message': 'fails vm positivity. Please ensure that the vm field of every bus is a positive real number',
                 'diagnostics': {
                     'i': self.i,
                     'vm': self.vm}})

    def check_nvhi_pos(self):

        if not (self.nvhi > 0.0):
            alert(
                {'data_type': 'Bus',
                 'error_message': 'fails nvhi positivity. Please ensure that the nvhi field of every bus is a positive real number',
                 'diagnostics': {
                     'i': self.i,
                     'nvhi': self.nvhi}})

    def check_nvlo_pos(self):

        if not (self.nvlo > 0.0):
            alert(
                {'data_type': 'Bus',
                 'error_message': 'fails nvlo positivity. Please ensure that the nvlo field of every bus is a positive real number',
                 'diagnostics': {
                     'i': self.i,
                     'nvlo': self.nvlo}})

    def check_evhi_pos(self):

        if not (self.evhi > 0.0):
            alert(
                {'data_type': 'Bus',
                 'error_message': 'fails evhi positivity. Please ensure that the evhi field of every bus is a positive real number',
                 'diagnostics': {
                     'i': self.i,
                     'evhi': self.evhi}})

    def check_evlo_pos(self):

        if not (self.evlo > 0.0):
            alert(
                {'data_type': 'Bus',
                 'error_message': 'fails evlo positivity. Please ensure that the evlo field of every bus is a positive real number',
                 'diagnostics': {
                     'i': self.i,
                     'evlo': self.evlo}})

    def check_nvhi_nvlo_consistent(self):

        if self.nvhi - self.nvlo < 0.0:
            alert(
                {'data_type': 'Bus',
                 'error_message': 'fails nvhi-nvlo consistency. Please ensure that the nvhi and nvlo fields of every bus satisfy: nvhi - nvlo >= 0.0',
                 'diagnostics': {
                     'i': self.i,
                     'nvhi - nvlo': (self.nvhi - self.nvlo),
                     'nvhi': self.nvhi,
                     'nvlo': self.nvlo}})

    def check_evhi_evlo_consistent(self):

        if self.evhi - self.evlo < 0.0:
            alert(
                {'data_type': 'Bus',
                 'error_message': 'fails evhi-evlo consistency. Please ensure that the evhi and evlo fields of every bus satisfy: evhi - evlo >= 0.0',
                 'diagnostics': {
                     'i': self.i,
                     'evhi - evlo': (self.evhi - self.evlo),
                     'evhi': self.evhi,
                     'evlo': self.evlo}})

    def check_evhi_nvhi_consistent(self):

        if self.evhi - self.nvhi < 0.0:
            alert(
                {'data_type': 'Bus',
                 'error_message': 'fails evhi-nvhi consistency. Please ensure that the evhi and nvhi fields of every bus satisfy: evhi - nvhi >= 0.0',
                 'diagnostics': {
                     'i': self.i,
                     'evhi - nvhi': (self.evhi - self.nvhi),
                     'evhi': self.evhi,
                     'nvhi': self.nvhi}})

    def check_nvlo_evlo_consistent(self):

        if self.nvlo - self.evlo < 0.0:
            alert(
                {'data_type': 'Bus',
                 'error_message': 'fails nvlo-evlo consistency. Please ensure that the nvlo and evlo fields of every bus satisfy: nvlo - evlo >= 0.0',
                 'diagnostics': {
                     'i': self.i,
                     'nvlo - evlo': (self.nvlo - self.evlo),
                     'nvlo': self.nvlo,
                     'evlo': self.evlo}})

    def read_from_row(self, row):

        row = pad_row(row, 13)
        self.i = parse_token(row[0], int, default=None)
        self.area = parse_token(row[4], int, default=None)
        self.vm = parse_token(row[7], float, default=None)
        self.va = parse_token(row[8], float, default=None)
        self.nvhi = parse_token(row[9], float, default=None)
        self.nvlo = parse_token(row[10], float, default=None)
        self.evhi = parse_token(row[11], float, default=None)
        self.evlo = parse_token(row[12], float, default=None)
        if read_unused_fields:
            self.name = parse_token(row[1], str, 12 * ' ')
            self.baskv = parse_token(row[2], float, 0.0)
            self.ide = parse_token(row[3], int, 1)
            self.zone = parse_token(row[5], int, 1)
            self.owner = parse_token(row[6], int, 1)


class Load:

    def __init__(self):

        self.i = None  # no default allowed - should be an error
        self.id = '1'
        self.status = 1
        self.area = 1  # default is area of bus self.i, but this is not available yet
        self.zone = 1
        self.pl = 0.0
        self.ql = 0.0
        self.ip = 0.0
        self.iq = 0.0
        self.yp = 0.0
        self.yq = 0.0
        self.owner = 1
        self.scale = 1
        self.intrpt = 0

    def check(self):

        self.check_id_len_1_or_2()
        # need to check i in buses

    def check_id_len_1_or_2(self):

        if not (len(self.id) in [1, 2]):
            alert(
                {'data_type': 'Load',
                 'error_message': 'fails id string len 1 or 2. Please ensure that the id field of every load is a 1- or 2-character string with no blank characters',
                 'diagnostics': {
                     'i': self.i,
                     'id': self.id}})

    def clean_id(self):
        '''remove spaces and non-allowed characters
        hope that this does not introduce duplication'''

        self.id = clean_short_str(self.id)

    def read_from_row(self, row):

        row = pad_row(row, 14)
        self.i = parse_token(row[0], int, default=None)
        self.id = parse_token(row[1], str, default=None).strip()
        self.status = parse_token(row[2], int, default=None)
        self.pl = parse_token(row[5], float, default=None)
        self.ql = parse_token(row[6], float, default=None)
        if read_unused_fields:
            self.area = parse_token(row[3], int, 1)
            self.zone = parse_token(row[4], int, 1)
            self.ip = parse_token(row[7], float, 0.0)
            self.iq = parse_token(row[8], float, 0.0)
            self.yp = parse_token(row[9], float, 0.0)
            self.yq = parse_token(row[10], float, 0.0)
            self.owner = parse_token(row[11], int, 1)
            self.scale = parse_token(row[12], int, 1)
            self.intrpt = parse_token(row[13], int, 0)


class FixedShunt:

    def __init__(self):

        self.i = None  # no default allowed
        self.id = '1'
        self.status = 1
        self.gl = 0.0
        self.bl = 0.0

    def check(self):

        self.check_id_len_1_or_2()
        # need to check i in buses

    def check_id_len_1_or_2(self):

        if not (len(self.id) in [1, 2]):
            alert(
                {'data_type': 'FixedShunt',
                 'error_message': 'fails id string len 1 or 2. Please ensure that the id field of every fixed shunt is a 1- or 2-character string with no blank characters',
                 'diagnostics': {
                     'i': self.i,
                     'id': self.id}})

    def read_from_row(self, row):

        row = pad_row(row, 5)
        self.i = parse_token(row[0], int, default=None)
        self.id = parse_token(row[1], str, default=None).strip()
        self.status = parse_token(row[2], int, default=None)
        self.gl = parse_token(row[3], float, default=None)
        self.bl = parse_token(row[4], float, default=None)
        if read_unused_fields:
            pass


class Generator:

    def __init__(self):
        self.i = None  # no default allowed
        self.id = '1'
        self.pg = 0.0
        self.qg = 0.0
        self.qt = 9999.0
        self.qb = -9999.0
        self.vs = 1.0
        self.ireg = 0
        self.mbase = 100.0  # need to take default value for this from larger Raw class
        self.zr = 0.0
        self.zx = 1.0
        self.rt = 0.0
        self.xt = 0.0
        self.gtap = 1.0
        self.stat = 1
        self.rmpct = 100.0
        self.pt = 9999.0
        self.pb = -9999.0
        self.o1 = 1
        self.f1 = 1.0
        self.o2 = 0
        self.f2 = 1.0
        self.o3 = 0
        self.f3 = 1.0
        self.o4 = 0
        self.f4 = 1.0
        self.wmod = 0
        self.wpf = 1.0

    def check(self):

        self.check_id_len_1_or_2()
        self.check_qt_qb_consistent()
        self.check_pt_pb_consistent()
        # check pg, qg within bounds?
        # need to check i in buses

    def check_id_len_1_or_2(self):

        if not (len(self.id) in [1, 2]):
            alert(
                {'data_type': 'Generator',
                 'error_message': 'fails id string len 1 or 2. Please ensure that the id field of every generator is a 1- or 2-character string with no blank characters',
                 'diagnostics': {
                     'i': self.i,
                     'id': self.id}})

    def check_qt_qb_consistent(self):

        if self.qt - self.qb < 0.0:
            alert(
                {'data_type': 'Generator',
                 'error_message': 'fails qt-qb consistency. Please ensure that the qt and qb fields of every generator satisfy: qt - qb >= 0.0',
                 'diagnostics': {
                     'i': self.i,
                     'id': self.id,
                     'qt - qb': (self.qt - self.qb),
                     'qt': self.qt,
                     'qb': self.qb}})

    def check_pt_pb_consistent(self):

        if self.pt - self.pb < 0.0:
            alert(
                {'data_type': 'Generator',
                 'error_message': 'fails pt-pb consistency. Please ensure that the pt and pb fields of every generator satisfy: pt - pb >= 0.0',
                 'diagnostics': {
                     'i': self.i,
                     'id': self.id,
                     'pt - pb': (self.pt - self.pb),
                     'pt': self.pt,
                     'pb': self.pb}})

    def read_from_row(self, row):

        row = pad_row(row, 28)
        self.i = parse_token(row[0], int, default=None)
        self.id = parse_token(row[1], str, default=None).strip()
        self.pg = parse_token(row[2], float, default=None)
        self.qg = parse_token(row[3], float, default=None)
        self.qt = parse_token(row[4], float, default=None)
        self.qb = parse_token(row[5], float, default=None)
        self.stat = parse_token(row[14], int, default=None)
        self.pt = parse_token(row[16], float, default=None)
        self.pb = parse_token(row[17], float, default=None)
        if read_unused_fields:
            self.vs = parse_token(row[6], float, 1.0)
            self.ireg = parse_token(row[7], int, 0)
            self.mbase = parse_token(row[8], float, 100.0)
            self.zr = parse_token(row[9], float, 0.0)
            self.zx = parse_token(row[10], float, 1.0)
            self.rt = parse_token(row[11], float, 0.0)
            self.xt = parse_token(row[12], float, 0.0)
            self.gtap = parse_token(row[13], float, 1.0)
            self.rmpct = parse_token(row[15], float, 100.0)
            self.o1 = parse_token(row[18], int, 1)
            self.f1 = parse_token(row[19], float, 1.0)
            self.o2 = parse_token(row[20], int, 0)
            self.f2 = parse_token(row[21], float, 1.0)
            self.o3 = parse_token(row[22], int, 0)
            self.f3 = parse_token(row[23], float, 1.0)
            self.o4 = parse_token(row[24], int, 0)
            self.f4 = parse_token(row[25], float, 1.0)
            self.wmod = parse_token(row[26], int, 0)
            self.wpf = parse_token(row[27], float, 1.0)


class NontransformerBranch:

    def __init__(self):

        self.i = None  # no default
        self.j = None  # no default
        self.ckt = '1'
        self.r = None  # no default
        self.x = None  # no default
        self.b = 0.0
        self.ratea = 0.0
        self.rateb = 0.0
        self.ratec = 0.0
        self.gi = 0.0
        self.bi = 0.0
        self.gj = 0.0
        self.bj = 0.0
        self.st = 1
        self.met = 1
        self.len = 0.0
        self.o1 = 1
        self.f1 = 1.0
        self.o2 = 0
        self.f2 = 1.0
        self.o3 = 0
        self.f3 = 1.0
        self.o4 = 0
        self.f4 = 1.0

    def check(self):

        self.check_ckt_len_1_or_2()
        self.check_r_x_nonzero()
        self.check_ratea_pos()
        self.check_ratec_pos()
        self.check_ratec_ratea_consistent()
        # need to check i, j in buses

    def check_ckt_len_1_or_2(self):

        if not (len(self.ckt) in [1, 2]):
            alert(
                {'data_type': 'NontransformerBranch',
                 'error_message': 'fails ckt string len 1 or 2. Please ensure that the id field of every nontransformer branch is a 1- or 2-character string with no blank characters',
                 'diagnostics': {
                     'i': self.i,
                     'j': self.j,
                     'ckt': self.ckt}})

    def check_r_x_nonzero(self):

        if (self.r == 0.0 and self.x == 0.0):
            alert(
                {'data_type': 'NontransformerBranch',
                 'error_message': 'fails r-x nonzero. Please ensure that at least one of the r and x fields of every nontransformer branch is nonzero. The competition formulation uses z = r + j*x, y = 1/z, g = Re(y), b = Im(y). This computation fails if r == 0.0 and x == 0.0.',
                 'diagnostics': {
                     'i': self.i,
                     'j': self.j,
                     'ckt': self.ckt,
                     'r:': self.r,
                     'x:': self.x}})

    def check_ratea_pos(self):

        if not (self.ratea > 0.0):
            alert(
                {'data_type': 'NontransformerBranch',
                 'error_message': 'fails ratea positivity. Please ensure that the ratea field of every nontransformer branch is a positivve real number.',
                 'diagnostics': {
                     'i': self.i,
                     'j': self.j,
                     'ckt': self.ckt,
                     'ratea': self.ratea}})

    def check_ratec_pos(self):

        if not (self.ratec > 0.0):
            alert(
                {'data_type': 'NontransformerBranch',
                 'error_message': 'fails ratec positivity. Please ensure that the ratec field of every nontransformer branch is a positivve real number.',
                 'diagnostics': {
                     'i': self.i,
                     'j': self.j,
                     'ckt': self.ckt,
                     'ratec': self.ratec}})

    def check_ratec_ratea_consistent(self):

        if self.ratec - self.ratea < 0.0:
            alert(
                {'data_type': 'NontransformerBranch',
                 'error_message': 'fails ratec-ratea consistency. Please ensure that the ratec and ratea fields of every nontransformer branch satisfy ratec - ratea >= 0.0.',
                 'diagnostics': {
                     'i': self.i,
                     'j': self.j,
                     'ckt': self.ckt,
                     'ratec - ratea': self.ratec - self.ratea,
                     'ratec': self.ratec,
                     'ratea': self.ratea}})

    def read_from_row(self, row):

        row = pad_row(row, 24)
        self.i = parse_token(row[0], int, default=None)
        self.j = parse_token(row[1], int, default=None)
        self.ckt = parse_token(row[2], str, default=None).strip()
        self.r = parse_token(row[3], float, default=None)
        self.x = parse_token(row[4], float, default=None)
        self.b = parse_token(row[5], float, default=None)
        self.ratea = parse_token(row[6], float, default=None)
        self.ratec = parse_token(row[8], float, default=None)
        self.st = parse_token(row[13], int, default=None)
        if read_unused_fields:
            self.rateb = parse_token(row[7], float, 0.0)
            self.gi = parse_token(row[9], float, 0.0)
            self.bi = parse_token(row[10], float, 0.0)
            self.gj = parse_token(row[11], float, 0.0)
            self.bj = parse_token(row[12], float, 0.0)
            self.met = parse_token(row[14], int, 1)
            self.len = parse_token(row[15], float, 0.0)
            self.o1 = parse_token(row[16], int, 1)
            self.f1 = parse_token(row[17], float, 1.0)
            self.o2 = parse_token(row[18], int, 0)
            self.f2 = parse_token(row[19], float, 1.0)
            self.o3 = parse_token(row[20], int, 0)
            self.f3 = parse_token(row[21], float, 1.0)
            self.o4 = parse_token(row[22], int, 0)
            self.f4 = parse_token(row[23], float, 1.0)


class Transformer:

    def __init__(self):

        self.i = None  # no default
        self.j = None  # no default
        self.k = 0
        self.ckt = '1'
        self.cw = 1
        self.cz = 1
        self.cm = 1
        self.mag1 = 0.0
        self.mag2 = 0.0
        self.nmetr = 2
        self.name = 12 * ' '
        self.stat = 1
        self.o1 = 1
        self.f1 = 1.0
        self.o2 = 0
        self.f2 = 1.0
        self.o3 = 0
        self.f3 = 1.0
        self.o4 = 0
        self.f4 = 1.0
        self.vecgrp = 12 * ' '
        self.r12 = 0.0
        self.x12 = None  # no default allowed
        self.sbase12 = 100.0
        self.windv1 = 1.0
        self.nomv1 = 0.0
        self.ang1 = 0.0
        self.rata1 = 0.0
        self.ratb1 = 0.0
        self.ratc1 = 0.0
        self.cod1 = 0
        self.cont1 = 0
        self.rma1 = 1.1
        self.rmi1 = 0.9
        self.vma1 = 1.1
        self.vmi1 = 0.9
        self.ntp1 = 33
        self.tab1 = 0
        self.cr1 = 0.0
        self.cx1 = 0.0
        self.cnxa1 = 0.0
        self.windv2 = 1.0
        self.nomv2 = 0.0

    def check(self):

        self.check_ckt_len_1_or_2()
        self.check_r12_x12_nonzero()
        self.check_rata1_pos()
        self.check_ratc1_pos()
        self.check_ratc1_rata1_consistent()
        self.check_windv1_pos()
        self.check_windv2_pos()
        self.check_windv2_eq_1()
        # need to check i, j in buses

    def check_ckt_len_1_or_2(self):

        if not (len(self.ckt) in [1, 2]):
            alert(
                {'data_type': 'Transformer',
                 'error_message': 'fails ckt string len 1 or 2. Please ensure that the ckt field of every transformer is a 1- or 2-character string with no blank characters',
                 'diagnostics': {
                     'i': self.i,
                     'j': self.j,
                     'k': self.k,
                     'ckt': self.ckt}})

    def check_r12_x12_nonzero(self):

        if (self.r12 == 0.0 and self.x12 == 0.0):
            alert(
                {'data_type': 'Transformer',
                 'error_message': 'fails r12-x12 nonzero. Please ensure that at least one of the r12 and x12 fields of every transformer is nonzero. The competition formulation uses z = r12 + j*x12, y = 1/z, g = Re(y), b = Im(y). This computation fails if r12 == 0.0 and x12 == 0.0.',
                 'diagnostics': {
                     'i': self.i,
                     'j': self.j,
                     'k': self.k,
                     'ckt': self.ckt,
                     'r12:': self.r12,
                     'x12:': self.x12}})

    def check_rata1_pos(self):

        if not (self.rata1 > 0.0):
            alert(
                {'data_type': 'Transformer',
                 'error_message': 'fails rata1 positivity. Please ensure that the rata1 field of every transformer is a positive real number.',
                 'diagnostics': {
                     'i': self.i,
                     'j': self.j,
                     'k': self.k,
                     'ckt': self.ckt,
                     'rata1': self.rata1}})

    def check_ratc1_pos(self):

        if not (self.ratc1 > 0.0):
            alert(
                {'data_type': 'Transformer',
                 'error_message': 'fails ratc1 positivity. Please ensure that the ratc1 field of every transformer is a positive real number.',
                 'diagnostics': {
                     'i': self.i,
                     'j': self.j,
                     'k': self.k,
                     'ckt': self.ckt,
                     'ratc1': self.ratc1}})

    def check_ratc1_rata1_consistent(self):

        if self.ratc1 - self.rata1 < 0.0:
            alert(
                {'data_type': 'Transformer',
                 'error_message': 'fails ratc1-rata1 consistency. Please ensure that the ratc1 and rata1 fields of every transformer satisfy ratc1 - rata1 >= 0.0.',
                 'diagnostics': {
                     'i': self.i,
                     'j': self.j,
                     'k': self.k,
                     'ckt': self.ckt,
                     'ratc1 - rata1': self.ratc1 - self.rata1,
                     'ratc1': self.ratc1,
                     'rata1': self.rata1}})

    def check_windv1_pos(self):

        if not (self.windv1 > 0.0):
            alert(
                {'data_type': 'Transformer',
                 'error_message': 'fails windv1 positivity. Please ensure that the windv1 field of every transformer is a positive real number.',
                 'diagnostics': {
                     'i': self.i,
                     'j': self.j,
                     'k': self.k,
                     'ckt': self.ckt,
                     'windv1': self.windv1}})

    def check_windv2_pos(self):

        if not (self.windv2 > 0.0):
            alert(
                {'data_type': 'Transformer',
                 'error_message': 'fails windv2 positivity. Please ensure that the windv2 field of every transformer is a positive real number.',
                 'diagnostics': {
                     'i': self.i,
                     'j': self.j,
                     'k': self.k,
                     'ckt': self.ckt,
                     'windv2': self.windv2}})

    def check_windv2_eq_1(self):

        if not (self.windv2 == 1.0):
            alert(
                {'data_type': 'Transformer',
                 'error_message': 'fails windv2 exactly equal to 1.0. Please ensure that the windv2 field of every transformer is equal to 1.0. Transformers not satisfying this property can be converted. This ensures that the formulation used by the Grid Optimization Competition is consistent with the model described in PSSE proprietary documentation',
                 'diagnostics': {
                     'i': self.i,
                     'j': self.j,
                     'k': self.k,
                     'ckt': self.ckt,
                     'windv2': self.windv2}})

    @property
    def num_windings(self):

        num_windings = 0
        if self.k is None:
            num_windings = 0
        elif self.k == 0:
            num_windings = 2
        else:
            num_windings = 3
        return num_windings

    def get_num_rows_from_row(self, row):

        num_rows = 0
        k = parse_token(row[2], int, 0)
        if k == 0:
            num_rows = 4
        else:
            num_rows = 5
        return num_rows

    def read_from_rows(self, rows):

        full_rows = self.pad_rows(rows)
        row = self.flatten_rows(full_rows)
        try:
            self.read_from_row(row)
        except Exception as e:
            print("row:")
            print(row)
            raise e

    def pad_rows(self, rows):

        return rows
        '''
        rows_new = rows
        if len(rows_new) == 4:
            rows_new.append([])
        rows_len = [len(r) for r in rows_new]
        rows_len_new = [21, 11, 17, 17, 17]
        rows_len_increase = [rows_len_new[i] - rows_len[i] for i in range(5)]
        # check no negatives in increase
        rows_new = [rows_new[i] + rows_len_increase[i]*[''] for i in range(5)]
        return rows_new
        '''

    def flatten_rows(self, rows):

        row = [t for r in rows for t in r]
        return row

    def read_from_row(self, row):

        # general (3- or 2-winding, 5- or 4-row)
        '''
        self.i = parse_token(row[0], int, '')
        self.j = parse_token(row[1], int, '')
        self.k = parse_token(row[2], int, 0)
        self.ckt = parse_token(row[3], str, '1')
        self.cw = parse_token(row[4], int, 1)
        self.cz = parse_token(row[5], int, 1)
        self.cm = parse_token(row[6], int, 1)
        self.mag1 = parse_token(row[7], float, 0.0)
        self.mag2 = parse_token(row[8], float, 0.0)
        self.nmetr = parse_token(row[9], int, 2)
        self.name = parse_token(row[10], str, 12*' ')
        self.stat = parse_token(row[11], int, 1)
        self.o1 = parse_token(row[12], int, 0)
        self.f1 = parse_token(row[13], float, 1.0)
        self.o2 = parse_token(row[14], int, 0)
        self.f2 = parse_token(row[15], float, 1.0)
        self.o3 = parse_token(row[16], int, 0)
        self.f3 = parse_token(row[17], float, 1.0)
        self.o4 = parse_token(row[18], int, 0)
        self.f4 = parse_token(row[19], float, 1.0)
        self.vecgrp = parse_token(row[20], str, 12*' ')
        self.r12 = parse_token(row[21], float, 0.0)
        self.x12 = parse_token(row[22], float, 0.0)
        self.sbase12 = parse_token(row[23], float, 0.0)
        self.r23 = parse_token(row[24], float, 0.0)
        self.x23 = parse_token(row[25], float, 0.0)
        self.sbase23 = parse_token(row[26], float, 0.0)
        self.r31 = parse_token(row[27], float, 0.0)
        self.x31 = parse_token(row[28], float, 0.0)
        self.sbase31 = parse_token(row[29], float, 0.0)
        self.vmstar = parse_token(row[30], float, 1.0)
        self.anstar = parse_token(row[31], float, 0.0)
        self.windv1 = parse_token(row[32], float, 1.0)
        self.nomv1 = parse_token(row[33], float, 0.0)
        self.ang1 = parse_token(row[34], float, 0.0)
        self.rata1 = parse_token(row[35], float, 0.0)
        self.ratb1 = parse_token(row[36], float, 0.0)
        self.ratc1 = parse_token(row[37], float, 0.0)
        self.cod1 = parse_token(row[38], int, 0)
        self.cont1 = parse_token(row[39], int, 0)
        self.rma1 = parse_token(row[40], float, 1.1)
        self.rmi1 = parse_token(row[41], float, 0.9)
        self.vma1 = parse_token(row[42], float, 1.1)
        self.vmi1 = parse_token(row[43], float, 0.9)
        self.ntp1 = parse_token(row[44], int, 33)
        self.tab1 = parse_token(row[45], int, 0)
        self.cr1 = parse_token(row[46], float, 0.0)
        self.cx1 = parse_token(row[47], float, 0.0)
        self.cnxa1 = parse_token(row[48], float, 0.0)
        self.windv2 = parse_token(row[49], float, 1.0)
        self.nomv2 = parse_token(row[50], float, 0.0)
        self.ang2 = parse_token(row[51], float, 0.0)
        self.rata2 = parse_token(row[52], float, 0.0)
        self.ratb2 = parse_token(row[53], float, 0.0)
        self.ratc2 = parse_token(row[54], float, 0.0)
        self.cod2 = parse_token(row[55], int, 0)
        self.cont2 = parse_token(row[56], int, 0)
        self.rma2 = parse_token(row[57], float, 1.1)
        self.rmi2 = parse_token(row[58], float, 0.9)
        self.vma2 = parse_token(row[59], float, 1.1)
        self.vmi2 = parse_token(row[60], float, 0.9)
        self.ntp2 = parse_token(row[61], int, 33)
        self.tab2 = parse_token(row[62], int, 0)
        self.cr2 = parse_token(row[63], float, 0.0)
        self.cx2 = parse_token(row[64], float, 0.0)
        self.cnxa2 = parse_token(row[65], float, 0.0)
        self.windv3 = parse_token(row[66], float, 1.0)
        self.nomv3 = parse_token(row[67], float, 0.0)
        self.ang3 = parse_token(row[68], float, 0.0)
        self.rata3 = parse_token(row[69], float, 0.0)
        self.ratb3 = parse_token(row[70], float, 0.0)
        self.ratc3 = parse_token(row[71], float, 0.0)
        self.cod3 = parse_token(row[72], int, 0)
        self.cont3 = parse_token(row[73], int, 0)
        self.rma3 = parse_token(row[74], float, 1.1)
        self.rmi3 = parse_token(row[75], float, 0.9)
        self.vma3 = parse_token(row[76], float, 1.1)
        self.vmi3 = parse_token(row[77], float, 0.9)
        self.ntp3 = parse_token(row[78], int, 33)
        self.tab3 = parse_token(row[79], int, 0)
        self.cr3 = parse_token(row[80], float, 0.0)
        self.cx3 = parse_token(row[81], float, 0.0)
        self.cnxa3 = parse_token(row[82], float, 0.0)
        '''

        # just 2-winding, 4-row
        try:
            if len(row) != 43:
                if len(row) < 43:
                    raise Exception('missing field not allowed')
                elif len(row) > 43:
                    row = remove_end_of_line_comment_from_row(row, '/')
                    if len(row) > new_row_len:
                        raise Exception('extra field not allowed')
        except Exception as e:
            traceback.print_exc()
            raise e
        self.i = parse_token(row[0], int, default=None)
        self.j = parse_token(row[1], int, default=None)
        self.ckt = parse_token(row[3], str, default=None).strip()
        # check no 3-winding
        k = parse_token(row[2], int, default=None)
        if not (k == 0):
            try:
                alert(
                    {'data_type': 'Transformer',
                     'error_message': 'fails no 3 winding transformer. Please model any 3 winding transformer as a configuration of 2 winding transformers',
                     'diagnostics': {
                         'i': self.i,
                         'j': self.j,
                         'k': k,
                         'ckt': self.ckt}})
                raise Exception('3 winding transformers not allowed')
            except Exception as e:
                traceback.print_exc()
                raise e
        self.mag1 = parse_token(row[7], float, default=None)
        self.mag2 = parse_token(row[8], float, default=None)
        self.stat = parse_token(row[11], int, default=None)
        self.r12 = parse_token(row[21], float, default=None)
        self.x12 = parse_token(row[22], float, default=None)
        self.windv1 = parse_token(row[24], float, default=None)
        self.ang1 = parse_token(row[26], float, default=None)
        self.rata1 = parse_token(row[27], float, default=None)
        self.ratc1 = parse_token(row[29], float, default=None)
        self.windv2 = parse_token(row[41], float, default=None)
        if read_unused_fields:
            self.k = parse_token(row[2], int, 0)
            self.cw = parse_token(row[4], int, 1)
            self.cz = parse_token(row[5], int, 1)
            self.cm = parse_token(row[6], int, 1)
            self.nmetr = parse_token(row[9], int, 2)
            self.name = parse_token(row[10], str, 12 * ' ')
            self.o1 = parse_token(row[12], int, 1)
            self.f1 = parse_token(row[13], float, 1.0)
            self.o2 = parse_token(row[14], int, 0)
            self.f2 = parse_token(row[15], float, 1.0)
            self.o3 = parse_token(row[16], int, 0)
            self.f3 = parse_token(row[17], float, 1.0)
            self.o4 = parse_token(row[18], int, 0)
            self.f4 = parse_token(row[19], float, 1.0)
            self.vecgrp = parse_token(row[20], str, 12 * ' ')
            self.sbase12 = parse_token(row[23], float, 0.0)
            self.nomv1 = parse_token(row[25], float, 0.0)
            self.ratb1 = parse_token(row[28], float, 0.0)
            self.cod1 = parse_token(row[30], int, 0)
            self.cont1 = parse_token(row[31], int, 0)
            self.rma1 = parse_token(row[32], float, 1.1)
            self.rmi1 = parse_token(row[33], float, 0.9)
            self.vma1 = parse_token(row[34], float, 1.1)
            self.vmi1 = parse_token(row[35], float, 0.9)
            self.ntp1 = parse_token(row[36], int, 33)
            self.tab1 = parse_token(row[37], int, 0)
            self.cr1 = parse_token(row[38], float, 0.0)
            self.cx1 = parse_token(row[39], float, 0.0)
            self.cnxa1 = parse_token(row[40], float, 0.0)
            self.nomv2 = parse_token(row[42], float, 0.0)


class Area:

    def __init__(self):

        self.i = None  # no default
        self.isw = 0
        self.pdes = 0.0
        self.ptol = 10.0
        self.arname = 12 * ' '

    def clean_arname(self):

        self.arname = ''

    def check(self):

        self.check_i_pos()

    def check_i_pos(self):

        if not (self.i > 0):
            alert(
                {'data_type': 'Area',
                 'error_message': 'fails i positivity. Please ensure that the i field of every area is a positive integer.',
                 'diagnostics': {
                     'i': self.i}})

    def read_from_row(self, row):

        row = pad_row(row, 5)
        self.i = parse_token(row[0], int, default=None)
        if read_unused_fields:
            self.isw = parse_token(row[1], int, 0)
            self.pdes = parse_token(row[2], float, 0.0)
            self.ptol = parse_token(row[3], float, 10.0)
            self.arname = parse_token(row[4], str, 12 * ' ')


class Zone:

    def __init__(self):

        self.i = None  # no default
        self.zoname = 12 * ' '

    def clean_zoname(self):

        self.zoname = ''

    def check(self):

        self.check_i_pos()

    def check_i_pos(self):

        if not (self.i > 0):
            alert(
                {'data_type': 'Zone',
                 'error_message': 'fails i positivity. Please ensure that the i field of every zone is a positive integer.',
                 'diagnostics': {
                     'i': self.i}})

    def read_from_row(self, row):

        row = pad_row(row, 2)
        self.i = parse_token(row[0], int, default=None)
        if read_unused_fields:
            self.zoname = parse_token(row[1], str, 12 * ' ')


class SwitchedShunt:

    def __init__(self):

        self.i = None  # no default
        self.modsw = 1
        self.adjm = 0
        self.stat = 1
        self.vswhi = 1.0
        self.vswlo = 1.0
        self.swrem = 0
        self.rmpct = 100.0
        self.rmidnt = 12 * ' '
        self.binit = 0.0
        self.n1 = 0
        self.b1 = 0.0
        self.n2 = 0
        self.b2 = 0.0
        self.n3 = 0
        self.b3 = 0.0
        self.n4 = 0
        self.b4 = 0.0
        self.n5 = 0
        self.b5 = 0.0
        self.n6 = 0
        self.b6 = 0.0
        self.n7 = 0
        self.b7 = 0.0
        self.n8 = 0
        self.b8 = 0.0

    def clean_rmidnt(self):

        self.rmidnt = ''

    def check(self):
        '''The Grid Optimization competition uses a continuous susceptance model
        of shunt switching. Therefore every switched shunt can be characterized by the following data:
          i     - bus number
          stat  - status
          b     - susceptance
          binit - susceptance at the operating point (starting point)
          bmin  - minimum susceptance value
          bmax  - maximum susceptance value

        b is not part of the raw file,
        but is instead part of the solution of the problem.
        a value of b should be determined by the solver
        for the base case (in solution1.txt)
        and for each contingency (in solution2.txt).

        i,stat,binit are provided directly by fields of those names in each record
        of the switched shunt section of the raw file.

        bmin and bmax are provided indirectly by several fields in each record of
        the switched shunt section of the raw file.

        The Grid Optimization Competition model requires bmin <= 0.0 and bmax >= 0.0/

        For simplicity, data providers should use nonzero values on a minimal set of n1, b1, ..., n8, b8,
        with the first 0 value terminating the data. I.e.
          n3 = 0
          b3 = 0.0
          n4 = 0
          b4 = 0.0
          n5 = 0
          b5 = 0.0
          n6 = 0
          b6 = 0.0
          n7 = 0
          b7 = 0.0
          n8 = 0
          b8 = 0.0
        if bmin < 0.0
            if bmax > 0.0
                n1 = 1
                n2 = 1
                {b1, b2} = {bmin, bmax}
            else
                n1 = 1
                n2 = 0
                b1 = bmin
                b2 = 0.0
        else
            if bmax > 0.0
                n1 = 1
                n2 = 0
                b1 = bmax
                b2 = 0.0
            else
                n1 = 0
                n2 = 0
                b1 = 0.0
                b2 = 0.0
        '''

        self.check_b1_b2_opposite_signs()
        self.check_n1_0_implies_b1_0_n2_0_b2_0()
        self.check_b1_0_implies_n1_0_n2_0_b2_0()
        self.check_n1_nonneg()
        self.check_n2_nonneg()
        self.check_n1_le_1()
        self.check_n2_le_1()
        self.check_n3_zero()
        self.check_n4_zero()
        self.check_n5_zero()
        self.check_n6_zero()
        self.check_n7_zero()
        self.check_n8_zero()
        self.check_b3_zero()
        self.check_b4_zero()
        self.check_b5_zero()
        self.check_b6_zero()
        self.check_b7_zero()
        self.check_b8_zero()
        self.check_bmin_le_binit()
        self.check_binit_le_bmax()

    def check_b1_b2_opposite_signs(self):

        if (((self.b1 < 0.0) and (self.b2 < 0.0)) or ((self.b1 > 0.0) and (self.b2 > 0.0))):
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails b1,b2 opposite sign requirement. For each switched shunt, please ensure that the fields b1, b2 are real numbers with opposite signs, i.e. if b1 < 0.0, then b2 >= 0.0, and if b1 > 0.0, then b2 <= 0.0. This is a minimal nonzero data requirement.',
                 'diagnostics': {
                     'i': self.i,
                     'n1': self.n1,
                     'b1': self.b1,
                     'n2': self.n2,
                     'b2': self.b2}})

    def check_n1_0_implies_b1_0_n2_0_b2_0(self):

        if ((self.n1 == 0) and ((self.b1 != 0.0) or (self.n2 != 0) or (self.b2 != 0.0))):
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails ((n1==0)->((b1==0.0)&(n2==0)&(b2==0.0))). For each switched shunt, please ensure that the fields n1, b1, n2, b2 satisfy this logical relation. This is a minimal nonzero data requirement.',
                 'diagnostics': {
                     'i': self.i,
                     'n1': self.n1,
                     'b1': self.b1,
                     'n2': self.n2,
                     'b2': self.b2}})

    def check_b1_0_implies_n1_0_n2_0_b2_0(self):

        if ((self.b1 == 0.0) and ((self.n1 != 0) or (self.n2 != 0) or (self.b2 != 0.0))):
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails ((b1==0.0)->((n1==0)&(n2==0)&(b2==0.0))). For each switched shunt, please ensure that the fields n1, b1, n2, b2 satisfy this logical relation. This is a minimal nonzero data requirement.',
                 'diagnostics': {
                     'i': self.i,
                     'n1': self.n1,
                     'b1': self.b1,
                     'n2': self.n2,
                     'b2': self.b2}})

    def check_n1_nonneg(self):

        if self.n1 < 0:
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails n1 nonnegativity. Please ensure that the n1 field of every switched shunt is a nonnegative integer.',
                 'diagnostics': {
                     'i': self.i,
                     'n1': self.n1}})

    def check_n2_nonneg(self):

        if self.n2 < 0:
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails n2 nonnegativity. Please ensure that the n2 field of every switched shunt is a nonnegative integer.',
                 'diagnostics': {
                     'i': self.i,
                     'n2': self.n2}})

    def check_n1_le_1(self):

        if self.n1 > 1:
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails n1 at most 1. Please ensure that the n1 field of every switched shunt is an integer <= 1.',
                 'diagnostics': {
                     'i': self.i,
                     'n1': self.n1}})

    def check_n2_le_1(self):

        if self.n2 > 1:
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails n2 at most 1. Please ensure that the n2 field of every switched shunt is an integer <= 1.',
                 'diagnostics': {
                     'i': self.i,
                     'n2': self.n2}})

    def check_n3_zero(self):

        if not (self.n3 == 0):
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails n3 exactly equal to 0. Please ensure that the n3 field of every switched shunt is exactly equal to 0. Since the Grid Optimization competition uses a continuous susceptance model of shunt switching, every switched shunt can be expressed using only the i,stat,binit,n1,b1,n2,b2 fields by means of a conversion.',
                 'diagnostics': {
                     'i': self.i,
                     'n3': self.n3}})

    def check_n4_zero(self):

        if not (self.n4 == 0):
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails n4 exactly equal to 0. Please ensure that the n4 field of every switched shunt is exactly equal to 0. Since the Grid Optimization competition uses a continuous susceptance model of shunt switching, every switched shunt can be expressed using only the i,stat,binit,n1,b1,n2,b2 fields by means of a conversion.',
                 'diagnostics': {
                     'i': self.i,
                     'n4': self.n4}})

    def check_n5_zero(self):

        if not (self.n5 == 0):
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails n5 exactly equal to 0. Please ensure that the n5 field of every switched shunt is exactly equal to 0. Since the Grid Optimization competition uses a continuous susceptance model of shunt switching, every switched shunt can be expressed using only the i,stat,binit,n1,b1,n2,b2 fields by means of a conversion.',
                 'diagnostics': {
                     'i': self.i,
                     'n5': self.n5}})

    def check_n6_zero(self):

        if not (self.n6 == 0):
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails n6 exactly equal to 0. Please ensure that the n6 field of every switched shunt is exactly equal to 0. Since the Grid Optimization competition uses a continuous susceptance model of shunt switching, every switched shunt can be expressed using only the i,stat,binit,n1,b1,n2,b2 fields by means of a conversion.',
                 'diagnostics': {
                     'i': self.i,
                     'n6': self.n6}})

    def check_n7_zero(self):

        if not (self.n7 == 0):
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails n7 exactly equal to 0. Please ensure that the n7 field of every switched shunt is exactly equal to 0. Since the Grid Optimization competition uses a continuous susceptance model of shunt switching, every switched shunt can be expressed using only the i,stat,binit,n1,b1,n2,b2 fields by means of a conversion.',
                 'diagnostics': {
                     'i': self.i,
                     'n7': self.n7}})

    def check_n8_zero(self):

        if not (self.n8 == 0):
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails n8 exactly equal to 0. Please ensure that the n8 field of every switched shunt is exactly equal to 0. Since the Grid Optimization competition uses a continuous susceptance model of shunt switching, every switched shunt can be expressed using only the i,stat,binit,n1,b1,n2,b2 fields by means of a conversion.',
                 'diagnostics': {
                     'i': self.i,
                     'n8': self.n8}})

    def check_b3_zero(self):

        if not (self.b3 == 0.0):
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails b3 exactly equal to 0.0. Please ensure that the b3 field of every switched shunt is exactly equal to 0.0. Since the Grid Optimization competition uses a continuous susceptance model of shunt switching, every switched shunt can be expressed using only the i,stat,binit,n1,b1,n2,b2 fields by means of a conversion.',
                 'diagnostics': {
                     'i': self.i,
                     'b3': self.b3}})

    def check_b4_zero(self):

        if not (self.b4 == 0.0):
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails b4 exactly equal to 0.0. Please ensure that the b4 field of every switched shunt is exactly equal to 0.0. Since the Grid Optimization competition uses a continuous susceptance model of shunt switching, every switched shunt can be expressed using only the i,stat,binit,n1,b1,n2,b2 fields by means of a conversion.',
                 'diagnostics': {
                     'i': self.i,
                     'b4': self.b4}})

    def check_b5_zero(self):

        if not (self.b5 == 0.0):
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails b5 exactly equal to 0.0. Please ensure that the b5 field of every switched shunt is exactly equal to 0.0. Since the Grid Optimization competition uses a continuous susceptance model of shunt switching, every switched shunt can be expressed using only the i,stat,binit,n1,b1,n2,b2 fields by means of a conversion.',
                 'diagnostics': {
                     'i': self.i,
                     'b5': self.b5}})

    def check_b6_zero(self):

        if not (self.b6 == 0.0):
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails b6 exactly equal to 0.0. Please ensure that the b6 field of every switched shunt is exactly equal to 0.0. Since the Grid Optimization competition uses a continuous susceptance model of shunt switching, every switched shunt can be expressed using only the i,stat,binit,n1,b1,n2,b2 fields by means of a conversion.',
                 'diagnostics': {
                     'i': self.i,
                     'b6': self.b6}})

    def check_b7_zero(self):

        if not (self.b7 == 0.0):
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails b7 exactly equal to 0.0. Please ensure that the b7 field of every switched shunt is exactly equal to 0.0. Since the Grid Optimization competition uses a continuous susceptance model of shunt switching, every switched shunt can be expressed using only the i,stat,binit,n1,b1,n2,b2 fields by means of a conversion.',
                 'diagnostics': {
                     'i': self.i,
                     'b7': self.b7}})

    def check_b8_zero(self):

        if not (self.b8 == 0.0):
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails b8 exactly equal to 0.0. Please ensure that the b8 field of every switched shunt is exactly equal to 0.0. Since the Grid Optimization competition uses a continuous susceptance model of shunt switching, every switched shunt can be expressed using only the i,stat,binit,n1,b1,n2,b2 fields by means of a conversion.',
                 'diagnostics': {
                     'i': self.i,
                     'b8': self.b8}})

    def check_bmin_le_binit(self):

        if min(self.b1, self.b2) > self.binit:
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails bmin <= binit. Please ensure that bmin <= binit, where bmin = min(b1,b2).',
                 'diagnostics': {
                     'i': self.i,
                     'bmin': min(self.b1, self.b2),
                     'binit': self.binit,
                     'b1': self.b1,
                     'b2': self.b2}})

    def check_binit_le_bmax(self):

        if self.binit > max(self.b1, self.b2):
            alert(
                {'data_type': 'SwitchedShunt',
                 'error_message': 'fails binit <= bmax. Please ensure that binit <= bmax, where bmax = max(b1,b2).',
                 'diagnostics': {
                     'i': self.i,
                     'bmax': max(self.b1, self.b2),
                     'binit': self.binit,
                     'b1': self.b1,
                     'b2': self.b2}})

    def read_from_row(self, row):

        row = pad_row(row, 26)
        self.i = parse_token(row[0], int, default=None)
        self.stat = parse_token(row[3], int, default=None)
        self.binit = parse_token(row[9], float, default=None)
        self.n1 = parse_token(row[10], int, default=None)
        self.b1 = parse_token(row[11], float, default=None)
        self.n2 = parse_token(row[12], int, default=None)
        self.b2 = parse_token(row[13], float, default=None)
        self.n3 = parse_token(row[14], int, default=None)
        self.b3 = parse_token(row[15], float, default=None)
        self.n4 = parse_token(row[16], int, default=None)
        self.b4 = parse_token(row[17], float, default=None)
        self.n5 = parse_token(row[18], int, default=None)
        self.b5 = parse_token(row[19], float, default=None)
        self.n6 = parse_token(row[20], int, default=None)
        self.b6 = parse_token(row[21], float, default=None)
        self.n7 = parse_token(row[22], int, default=None)
        self.b7 = parse_token(row[23], float, default=None)
        self.n8 = parse_token(row[24], int, default=None)
        self.b8 = parse_token(row[25], float, default=None)
        if read_unused_fields:
            self.modsw = parse_token(row[1], int, 1)
            self.adjm = parse_token(row[2], int, 0)
            self.vswhi = parse_token(row[4], float, 1.0)
            self.vswlo = parse_token(row[5], float, 1.0)
            self.swrem = parse_token(row[6], int, 0)
            self.rmpct = parse_token(row[7], float, 100.0)
            self.rmidnt = parse_token(row[8], str, 12 * ' ')


class GeneratorDispatchRecord:

    def __init__(self):
        self.bus = None  # no default allowed
        self.genid = None  # no default allowed
        self.disp = 1.0
        self.dsptbl = None  # no default allowed

    def check(self):
        pass
        # need to check that bus,genid is in generators
        # need to check that dsptbl is in the active power dispatch records
        # need to check that every generrator has a generator dispatch record

    def read_from_row(self, row):
        row = pad_row(row, 4)
        self.bus = parse_token(row[0], int, default=None)
        self.genid = parse_token(row[1], str, default=None).strip()
        self.dsptbl = parse_token(row[3], int, default=None)
        if read_unused_fields:
            self.disp = parse_token(row[2], float, 1.0)

    def read_from_csv(self, row):
        self.bus = parse_token(row[0], int, default=None)
        self.genid = parse_token(row[1], str, default=None).strip()


class ActivePowerDispatchRecord:

    def __init__(self):

        self.tbl = None  # no default allowed
        self.pmax = 9999.0
        self.pmin = -9999.0
        self.fuelcost = 1.0
        self.ctyp = 1
        self.status = 1
        self.ctbl = None  # no default allowed

    def check(self):

        self.check_tbl_pos()
        # need to check that ctbl is in the piecewise linear cost functions

    def check_tbl_pos(self):

        if not (self.tbl > 0):
            alert(
                {'data_type': 'ActivePowerDispatchRecord',
                 'error_message': 'fails tbl positivity. Please ensure that the tbl field of every active power dispatch record is a positive integer',
                 'diagnostics': {
                     'tbl': self.tbl}})

    def read_from_row(self, row):

        row = pad_row(row, 7)
        self.tbl = parse_token(row[0], int, default=None)
        self.ctbl = parse_token(row[6], int, default=None)
        if read_unused_fields:
            self.pmax = parse_token(row[1], float, 9999.0)
            self.pmin = parse_token(row[2], float, -9999.0)
            self.fuelcost = parse_token(row[3], float, 1.0)
            self.ctyp = parse_token(row[4], int, 1)
            self.status = parse_token(row[5], int, 1)


class PiecewiseLinearCostFunction():

    def __init__(self):

        self.ltbl = None  # no default value allowed
        self.label = ''
        self.npairs = None  # no default value allowed
        self.points = []  # no default value allowed

    def check(self):

        self.check_ltbl_pos()
        self.check_npairs_eq_len_points()
        self.check_at_least_two_points()
        self.check_dx_margin()
        self.check_ddydx_margin()

    def check_ltbl_pos(self):

        if not (self.ltbl > 0):
            alert(
                {'data_type': 'PiecewiseLinearCostFunction',
                 'error_message': 'fails ltbl positivity. Please ensure that the ltbl field of every piecewise linear cost function is a positive integer',
                 'diagnostics': {
                     'ltbl': self.ltbl}})

    def check_npairs_eq_len_points(self):

        num_points = len(self.points)
        if not (self.npairs == num_points):
            alert(
                {'data_type': 'PiecewiseLinearCostFunction',
                 'error_message': 'fails npairs exactly equal to number of points. Please ensure that for each piecewise linear cost function, the npairs field is an integer equal to the number of points provided.',
                 'diagnostics': {
                     'ltbl': self.ltbl,
                     'npairs': self.npairs,
                     'nx': num_points}})

    def check_at_least_two_points(self):

        num_points = len(self.points)
        if num_points < 2:
            alert(
                {'data_type': 'PiecewiseLinearCostFunction',
                 'error_message': 'fails to have at least 2 points. Please provide at least 2 sample points for each piecewise linear cost function',
                 'diagnostics': {
                     'ltbl': self.ltbl,
                     'nx': num_points}})

    def check_dx_margin(self):

        num_points = len(self.points)
        x = [self.points[i].x for i in range(num_points)]
        dx = [x[i + 1] - x[i] for i in range(num_points - 1)]
        for i in range(num_points - 1):
            if dx[i] < gen_cost_dx_margin:
                alert(
                    {'data_type': 'PiecewiseLinearCostFunction',
                     'error_message': (
                             'fails dx margin at points (i, i + 1). Please ensure that the sample points on each piecewise linear cost function are listed in order of increasing x coordinate, with each consecutive pair of x points differing by at least: %10.2e (MW)' %
                             gen_cost_dx_margin),
                     'diagnostics': {
                         'ltbl': self.ltbl,
                         'i': i,
                         'dx[i]': dx[i],
                         'x[i]': x[i],
                         'x[i + 1]': x[i + 1]}})

    def check_ddydx_margin(self):

        num_points = len(self.points)
        x = [self.points[i].x for i in range(num_points)]
        y = [self.points[i].y for i in range(num_points)]
        dx = [x[i + 1] - x[i] for i in range(num_points - 1)]
        dy = [y[i + 1] - y[i] for i in range(num_points - 1)]
        dydx = [dy[i] / dx[i] for i in range(num_points - 1)]
        ddydx = [dydx[i + 1] - dydx[i] for i in range(num_points - 2)]
        for i in range(num_points - 2):
            if ddydx[i] < gen_cost_ddydx_margin:
                alert(
                    {'data_type': 'PiecewiseLinearCostFunction',
                     'error_message': (
                             'fails ddydx margin at points (i, i + 1, i + 2). Please ensure that the sample points on each piecewise linear cost function have increasing slopes, with each consecutive pair of slopese differing by at least: %10.2e (USD/MW-h)' %
                             gen_cost_ddydx_margin),
                     'diagnostics': {
                         'ltbl': self.ltbl,
                         'i': i,
                         'ddydx[i]': ddydx[i],
                         'dydx[i]': dydx[i],
                         'dydx[i + 1]': dydx[i + 1],
                         'x[i]': x[i],
                         'x[i + 1]': x[i + 1],
                         'x[i + 2]': x[i + 2],
                         'y[i]': y[i],
                         'y[i + 1]': y[i + 1],
                         'y[i + 2]': y[i + 2]}})

    def check_x_min_margin(self, pmin):

        num_points = len(self.points)
        x = [self.points[i].x for i in range(num_points)]
        xmin = min(x)
        if pmin - xmin < gen_cost_x_bounds_margin:
            alert(
                {'data_type': 'PiecewiseLinearCostFunction',
                 'error_message': (
                         'fails x min margin. Please ensure that for each piecewise linear cost function f for a generator g, the x coordinate of at least one of the sample points of f is less than pmin of g by at least: %10.2e (MW)' %
                         gen_cost_x_bounds_margin),
                 'diagnostics': {
                     'ltbl': self.ltbl,
                     'pmin - xmin': (pmin - xmin),
                     'pmin': pmin,
                     'xmin': xmin}})

    def check_x_max_margin(self, pmax):

        num_points = len(self.points)
        x = [self.points[i].x for i in range(num_points)]
        xmax = max(x)
        if xmax - pmax < gen_cost_x_bounds_margin:
            alert(
                {'data_type': 'PiecewiseLinearCostFunction',
                 'error_message': (
                         'fails x max margin. Please ensure that for each piecewise linear cost function f for a generator g, the x coordinate of at least one of the sample points of f is greater than pmax of g by at least: %10.2e (MW)' %
                         gen_cost_x_bounds_margin),
                 'diagnostics': {
                     'ltbl': self.ltbl,
                     'xmax - pmax': (xmax - pmax),
                     'pmax': pmax,
                     'xmax': xmax}})

    def read_from_row(self, row):

        self.ltbl = parse_token(row[0], int, default=None)
        self.npairs = parse_token(row[2], int, default=None)
        for i in range(self.npairs):
            point = Point()
            point.read_from_row(
                row[(3 + 2 * i):(5 + 2 * i)])
            self.points.append(point)
        if read_unused_fields:
            self.label = parse_token(row[1], str, '').strip()

    def get_num_rows_from_row(self, row):

        num_rows = parse_token(row[2], int, 0) + 1
        return num_rows

    def flatten_rows(self, rows):

        row = [t for r in rows for t in r]
        return row

    def read_from_rows(self, rows):

        self.read_from_row(self.flatten_rows(rows))


class QuadraticCostFunctions(GeneratorDispatchRecord, PiecewiseLinearCostFunction):
    def __init__(self):
        GeneratorDispatchRecord.__init__(self)
        PiecewiseLinearCostFunction.__init__(self)
        self.constc = None
        self.linearc = None
        self.quadraticc = None
        self.powerfactor = None

    def check(self):

        pass

    def read_from_csv_quadraticinfo(self, row):
        if parse_token(row[2], int, '') == 0:
            self.constc = parse_token(row[3], float, 0.0)
        elif parse_token(row[2], int, '') == 1:
            self.linearc = parse_token(row[3], float, 0.0)
        elif parse_token(row[2], int, '') == 2:
            self.quadraticc = parse_token(row[3], float, 0.0)
        elif parse_token(row[2], int, '') == 9:
            self.powerfactor = parse_token(row[3], float, 0.0)


class GeneratorInlRecord:

    def __init__(self):
        self.i = None  # no default allowed
        self.id = None  # no default allowed
        self.h = 4.0
        self.pmax = 0.0
        self.pmin = 0.0
        self.r = 0.05
        self.d = 0.0

    def check(self):
        pass
        # ???
        # need to check (i,id) is in the generators
        # need to check that every generator is in the INL file
        # need to check (i,j,k,ckt) consistency between lines and transformers

    def read_from_row(self, row):
        row = pad_row(row, 7)
        self.i = parse_token(row[0], int, default=None)
        self.id = parse_token(row[1], str, default=None).strip()
        self.r = parse_token(row[5], float, default=None)
        if read_unused_fields:
            self.h = parse_token(row[2], float, 4.0)
            self.pmax = parse_token(row[3], float, 1.0)
            self.pmin = parse_token(row[4], float, 0.0)
            self.d = parse_token(row[6], float, 0.0)


class Contingency:

    def __init__(self):

        self.label = ''
        self.branch_out_events = []
        self.generator_out_events = []

    def check(self):

        self.check_label()
        self.check_branch_out_events()
        self.check_generator_out_events()
        self.check_at_most_one_branch_out_event()
        self.check_at_most_one_generator_out_event()
        self.check_at_most_one_branch_or_generator_out_event()
        self.check_at_least_one_branch_or_generator_out_event()
        # need to check that each outaged component is active in the base case

    def clean_label(self):
        '''remove spaces and non-allowed characters
        better to just give each contingency a label that is a positive integer'''

        pass

    def check_label(self):
        '''check that there are no spaces or non-allowed characters'''

        pass

    def check_branch_out_events(self):

        for r in self.branch_out_events:
            r.check()

    def check_generator_out_events(self):

        for r in self.generator_out_events:
            r.check()

    def check_at_most_one_branch_out_event(self):

        if len(self.branch_out_events) > 1:
            alert(
                {'data_type': 'Contingency',
                 'error_message': 'fails at most 1 branch out event. Please ensure that each contingency has at most 1 branch out event.',
                 'diagnostics': {
                     'label': self.label,
                     'num branch out events': len(self.branch_out_events)}})

    def check_at_most_one_generator_out_event(self):

        if len(self.generator_out_events) > 1:
            alert(
                {'data_type': 'Contingency',
                 'error_message': 'fails at most 1 generator out event. Please ensure that each contingency has at most 1 generator out event.',
                 'diagnostics': {
                     'label': self.label,
                     'num generator out events': len(self.generator_out_events)}})

    def check_at_most_one_branch_or_generator_out_event(self):

        if len(self.branch_out_events) + len(self.generator_out_events) > 1:
            alert(
                {'data_type': 'Contingency',
                 'error_message': 'fails at most 1 branch or generator out event. Please ensure that each contingency has at most 1 branch or generator out event.',
                 'diagnostics': {
                     'label': self.label,
                     'num branch out events + num generator out events': len(self.branch_out_events) + len(self.generator_out_events)}})

    def check_at_least_one_branch_or_generator_out_event(self):

        if len(self.branch_out_events) + len(self.generator_out_events) < 1:
            alert(
                {'data_type': 'Contingency',
                 'error_message': 'fails at least 1 branch or generator out event. Please ensure that each contingency has at least 1 branch or generator out event.',
                 'diagnostics': {
                     'label': self.label,
                     'num branch out events + num generator out events': len(self.branch_out_events) + len(self.generator_out_events)}})

    def construct_record_rows(self):

        rows = (
                [['CONTINGENCY', self.label]] +
                [r.construct_record_row()
                 for r in self.branch_out_events] +
                [r.construct_record_row()
                 for r in self.generator_out_events] +
                [['END']])
        return rows


class Point:

    def __init__(self):
        self.x = None
        self.y = None

    def check(self):
        pass

    def read_from_row(self, row):
        row = pad_row(row, 2)
        self.x = parse_token(row[0], float, default=None)
        self.y = parse_token(row[1], float, default=None)


class BranchOutEvent:

    def __init__(self):
        self.i = None
        self.j = None
        self.ckt = None

    def check(self):
        pass
        # need to check (i,j,ckt) is either a line or a transformer
        # need to check that it is active in the base case

    def read_from_row(self, row):
        check_row_missing_fields(row, 10)
        self.i = parse_token(row[4], int, default=None)
        self.j = parse_token(row[7], int, default=None)
        self.ckt = parse_token(row[9], str, default=None).strip()

    def read_from_csv(self, row):
        self.i = parse_token(row[2], int, '')
        self.j = parse_token(row[3], int, '')
        self.ckt = parse_token(row[4], str, '1')

    '''
    def read_three_winding_from_row(self, row):

        row = pad_row(row, 13)
        self.i = parse_token(row[4], int, '')
        self.j = parse_token(row[7], int, '')
        self.k = parse_token(row[10], int, '')
        self.ckt = parse_token(row[12], str, '1')
    '''

    def construct_record_row(self):
        return ['OPEN', 'BRANCH', 'FROM', 'BUS', self.i, 'TO', 'BUS', self.j, 'CIRCUIT', self.ckt]


class GeneratorOutEvent:

    def __init__(self):
        self.i = None
        self.id = None

    def check(self):
        pass
        # need to check that (i,id) is a generator and that it is active in the base case

    def read_from_csv(self, row):
        self.i = parse_token(row[2], int, '')
        self.id = parse_token(row[3], str, '')

    def read_from_row(self, row):
        self.i = parse_token(row[5], int, default=None)
        self.id = parse_token(row[2], str, default=None).strip()

    def construct_record_row(self):
        return ['REMOVE', 'UNIT', self.id, 'FROM', 'BUS', self.i]


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
    # p = data_read.Data()
    p = Data()
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
        mvar_ = 0.0
        if bus_ in sw_dict:
            mvar_ = g_results.loc[sw_dict[bus_], 'q_mvar'] / (b_results.loc[bus_, 'voltage_pu'] ** 2)
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
        mvar_ = 0.0
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
    # deltapgens = p_delta + pgen_out
    write_csvdata(fname, [], [['--delta section'], ['delta_p'], [delta]])
    return


def adj_swshunt_susceptance(_net, _swshidx_dict, _swsh_q_mins, _swsh_q_maxs):
    for _shbus in _swshidx_dict:                                                    # loop across swshunt (gen) buses
        _shidx = _swshidx_dict[_shbus]                                              # get swshunt index
        _busv = _net.res_bus.loc[_shbus, 'vm_pu']                                   # get swshunt bus voltage
        _maxq = _swsh_q_maxs[_shidx]
        _mvar = _net.res_gen.loc[_shidx, 'q_mvar']                                  # get swshunt vars
        if _busv > 1.0:                                                                              # IF BUS VOLTAGE > 1.0 PU...
            _next_mvar = _maxq * _busv ** 2
            _net.gen.loc[_shidx, 'max_q_mvar'] = _next_mvar                                          # SET MIN SWSHUNT VARS
        elif _busv < 1.0:                                                                            # IF BUS VOLTAGE < 1.0 PU...
            _next_mvar = _maxq * _busv ** 2
            _net.gen.loc[_shidx, 'min_q_mvar'] = _next_mvar                                          # SET MAX SWSHUNT VARS
        # print(_busv > 1.0, _shbus, _busv, _mvar, _next_mvar, _maxq)
    return _net


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


def get_generation_cost(_net, _gen_dict, _pwlcost_dict0):
    _cost = 0.0
    for _gkey in _pwlcost_dict0:                                                                   # LOOP ACROSS PARTICIPATING GENERATORS
        _gidx = _gen_dict[_gkey]
        _pcostdata = _pwlcost_dict0[_gkey]
        _g_mw = _net.res_gen.loc[_gidx, 'p_mw']
        _xlist, _ylist = zip(*_pcostdata)
        _cost += numpy.interp(_g_mw, _xlist, _ylist)
    return _cost


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
    swingbus1, sw1_genkey = get_alt_swingbus(raw_data.raw.generators, raw_data.raw.buses, swingbus, swing_kv, swgens_data[0][6])

    # =============================================================================================
    # == CREATE NETWORK ===========================================================================
    # =============================================================================================
    print('------------------------ CREATING NETWORKS -------------------------')
    create_starttime = time.time()
    net_a = pp.create_empty_network('net_a', 60.0, mva_base)
    net_c = pp.create_empty_network('net_c', 60.0, mva_base)

    # == ADD BUSES TO NETWORK =====================================================================
    # buses = (bus.i, bus.ide, bus.baskv, bus.area, bus.vm, bus.va, bus.nvhi, bus.nvlo, bus.evhi, bus.evlo)
    print('ADD BUSES ..........................................................')
    busnomkv_dict = {}
    buskv_dict = {}
    busarea_dict = {}
    busidxs = []
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
        pp.create_bus(net_a, vn_kv=busnomkv, zone=busarea, max_vm_pu=bus.nvhi, min_vm_pu=bus.nvlo, index=busnum)
        # -- CONTINGENCY NETWORK ------------------------------------------------------------------
        idx = pp.create_bus(net_c, vn_kv=busnomkv, zone=busarea, max_vm_pu=bus.evhi, min_vm_pu=bus.evlo, index=busnum)
        if busnum == swingbus:
            swingbus_idx = idx
        busnomkv_dict.update({busnum: busnomkv})
        buskv_dict.update({busnum: buskv})
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
        loadp = load.pl
        loadq = load.ql
        loadmva = math.sqrt(loadp ** 2 + loadq ** 2)
        pp.create_load(net_a, bus=loadbus, p_mw=loadp, q_mvar=loadq, sn_mva=loadmva, name=loadname)
        pp.create_load(net_c, bus=loadbus, p_mw=loadp, q_mvar=loadq, sn_mva=loadmva, name=loadname)

    # == ADD GENERATORS TO NETWORK ================================================================
    print('ADD GENERATORS .....................................................')
    genbuses = []
    gids = []
    gen_dict = {}
    genidx_dict = {}
    swinggen_idxs = []
    gen_status_vreg_dict = {}
    genarea_dict = {}
    genidxs = []
    genbus_dict = {}
    participating_gens = []
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
        vreg = SwVreg_Init
        status = swgen_data[9]
        nomkv = busnomkv_dict[genbus]
        genmva = math.sqrt(pmax ** 2 + qmax ** 2)
        power_factor = pmax / genmva
        gen_status_vreg_dict.update({genbus: [True, vreg]})
        if genkey in pwlcost_dict:
            pcostdata = pwlcost_dict[genkey]
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin,  max_q_mvar=qmax, min_q_mvar=qmin,
                                rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv, type='SWGEN', controllable=True, in_service=status)
            pp.create_pwl_cost(net_a, idx, 'gen', pcostdata)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, sn_mva=genmva, name=genkey, max_p_mw=pmax, min_p_mw=pmin,  max_q_mvar=qmax, min_q_mvar=qmin,
                          rdss_pu=0.005, xdss_pu=0.25,  cos_phi=power_factor, vn_kv=nomkv, type='SWGEN', controllable=True, in_service=status, index=idx)
            pp.create_pwl_cost(net_c, idx, 'gen', pcostdata)
            if status:
                participating_gens.append(genkey)
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
        genidx_dict.update({genbus: idx})
        genbuses.append(genbus)
        gids.append("'" + gid + "'")
        genarea_dict.update({genkey: busarea_dict[genbus]})
        genidxs.append(idx)
        genbus_dict.update({genkey: genbus})

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
        gen_status_vreg_dict.update({genbus: [False, vreg]})
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
        else:
            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_gen(net_a, genbus, pgen, vm_pu=vreg, name=genkey, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=qmax, min_q_mvar=qmin,
                                rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv,  type='GEN', controllable=False, in_service=status)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_gen(net_c, genbus, pgen, vm_pu=vreg, name=genkey, max_p_mw=pmax, min_p_mw=pmin, max_q_mvar=qmax, min_q_mvar=qmin,
                          rdss_pu=0.005, xdss_pu=0.25, cos_phi=power_factor, vn_kv=nomkv,  type='GEN', controllable=False, in_service=status, index=idx)
        if status:
            gen_status_vreg_dict[genbus][0] = status
        gids.append("'" + gid + "'")
        gen_dict.update({genkey: idx})
        genidx_dict.update({genbus: idx})
        genbuses.append(genbus)
        genarea_dict.update({genkey: busarea_dict[genbus]})
        genidxs.append(idx)
        genbus_dict.update({genkey: genbus})

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
            mw = fxshunt.gl
            mvar = fxshunt.bl
            nomkv = busnomkv_dict[shuntbus]

            # -- BASE NETWORK ---------------------------------------------------------------------
            idx = pp.create_shunt(net_a, shuntbus, vn_kv=nomkv, q_mvar=fxshunt.bl, p_mw=fxshunt.gl, step=1, max_step=True, name=shuntname)
            # -- CONTINGENCY NETWORK --------------------------------------------------------------
            pp.create_shunt(net_c, shuntbus, vn_kv=nomkv, q_mvar=fxshunt.bl, p_mw=fxshunt.gl, step=1, max_step=True, name=shuntname, index=idx)
            fxidxdict.update({shuntbus: idx})

    # == ADD SWITCHED SHUNTS TO NETWORK ===========================================================
    # -- (SWSHUNTS ARE MODELED AS Q-GENERATORS) ---------------------------------------------------
    # swshunt = (swshunt.i, swshunt.binit, swshunt.n1, swshunt.b1, swshunt.n2, swshunt.b2, swshunt.n3, swshunt.b3, swshunt.n4, swshunt.b4,
    #            swshunt.n5, swshunt.b5, swshunt.n6, swshunt.b6, swshunt.n7, swshunt.b7, swshunt.n8, swshunt.b8, swshunt.stat)
    # gens = (gen.i, gen.id, gen.pg, gen.qg, gen.qt, gen.qb, gen.pt, gen.pb, gen.stat)
    swshidx_dict = {}
    swshidxs = []
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
            if shuntbus in gen_status_vreg_dict:
                if gen_status_vreg_dict[shuntbus][0]:
                    vreg = gen_status_vreg_dict[shuntbus][1]
            steps = [swshunt.n1, swshunt.n2, swshunt.n3, swshunt.n4, swshunt.n5, swshunt.n6, swshunt.n7, swshunt.n8]
            kvars = [swshunt.b1, swshunt.b2, swshunt.b3, swshunt.b4, swshunt.b5, swshunt.b6, swshunt.b7, swshunt.b8]
            total_qmin = 0.0
            total_qmax = 0.0
            for j in range(len(kvars)):
                if kvars[j] < 0.0:
                    total_qmin += steps[j] * kvars[j]
                elif kvars[j] > 0.0:
                    total_qmax += steps[j] * kvars[j]
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

    # == ADD LINES TO NETWORK =====================================================================
    # line = (line.i, line.j, line.ckt, line.r, line.x, line.b, line.ratea, line.ratec, line.st)
    line_dict = {}
    line_ratea_dict = {}
    lineidxs = []
    print('ADD LINES ..........................................................')
    for line in raw_data.raw.nontransformer_branches.values():
        frombus = line.i
        tobus = line.j
        ckt = line.ckt
        status = bool(line.st)
        length = 1.0
        kv = busnomkv_dict[frombus]
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
        line_dict.update({linekey: idx})
        lineidxs.append(idx)

    # == ADD 2W TRANSFORMERS TO NETWORK ===========================================================
    # 2wxfmr = (xfmr.i, xfmr.j, xfmr.ckt, xfmr.mag1, xfmr.mag2, xfmr.r12, xfmr.x12, xfmr.windv1, xfmr.ang1, xfmr.rata1, xfmr.ratc1, xfmr.windv2, xfmr.stat)
    xfmr_dict = {}
    xfmr_ratea_dict = {}
    xfmridxs = []
    print('ADD 2W TRANSFORMERS ................................................')
    for xfmr in raw_data.raw.transformers.values():
        status = bool(xfmr.stat)
        frombus = xfmr.i
        tobus = xfmr.j
        ckt = xfmr.ckt
        xfmrkey = str(frombus) + '-' + str(tobus) + '-' + ckt
        fromkv = busnomkv_dict[frombus]
        tokv = busnomkv_dict[tobus]
        if fromkv < tokv:                           # force from bus to be highside
            frombus, tobus = tobus, frombus
            fromkv, tokv = tokv, fromkv
        r_pu_sbase = xfmr.r12                             # @ mva_base
        x_pu_sbase = xfmr.x12                             # @ mva_base
        r_pu = r_pu_sbase * xfmr.rata1 / mva_base   # pandapower uses given transformer rating as test mva
        x_pu = x_pu_sbase * xfmr.rata1 / mva_base   # so convert to mva_rating base
        z_pu = math.sqrt(r_pu ** 2 + x_pu ** 2)  # calculate 'nameplate' pu impedance
        z_pct = 100.0 * z_pu                      # pandadower uses percent impedance
        r_pct = 100.0 * r_pu                      # pandadower uses percent resistance
        noloadlosses = 0.0
        ironlosses = 0.0
        # -- TAP SETTINGS -----------------------------------------------------
        shiftdegree = 0.0
        tapside = 'hv'
        tappos = 3
        tapnuetral = 3
        tapmax = 5
        tapmin = 1
        tapsteppercent = 2.5
        tapstepdegree = 0.0
        tapphaseshifter = False
        # -- BASE NETWORK -------------------------------------------------------------------------
        idx = pp.create_transformer_from_parameters(net_a, frombus, tobus, xfmr.rata1, fromkv, tokv, r_pct, z_pct, ironlosses, noloadlosses,
                                                    shift_degree=shiftdegree, tap_side=tapside, tap_neutral=tapnuetral, tap_max=tapmax, tap_min=tapmin,
                                                    tap_step_percent=tapsteppercent, tap_step_degree=tapstepdegree, tap_pos=tappos, tap_phase_shifter=False,
                                                    in_service=status, name=xfmrkey, max_loading_percent=100.0, parallel=1, df=1.0)

        # -- CONTINGENCY NETWORK ------------------------------------------------------------------
        pp.create_transformer_from_parameters(net_c, frombus, tobus, xfmr.rata1, fromkv, tokv, r_pct, z_pct, ironlosses, noloadlosses,
                                              shift_degree=shiftdegree, tap_side=tapside, tap_neutral=tapnuetral, tap_max=tapmax, tap_min=tapmin,
                                              tap_step_percent=tapsteppercent, tap_step_degree=tapstepdegree, tap_pos=tappos, tap_phase_shifter=False,
                                              in_service=status, name=xfmrkey, max_loading_percent=100.0, parallel=1, df=1.0, index=idx)
        xfmr_dict.update({xfmrkey: idx})
        xfmr_ratea_dict.update({xfmrkey: xfmr.rata1})
        xfmridxs.append(idx)

    # == ADD EXTERNAL GRID ========================================================================
    # == WITH DUMMY TIE TO RAW SWING BUS ==========================================================
    # == DUMMY TIE RATING UP WITH LOWER KV ========================================================
    ext_tie_rating = 1e5/(math.sqrt(3) * swing_kv)

    # -- CREATE BASE NETWORK EXTERNAL GRID --------------------------------------------------------
    ext_grid_idx = pp.create_bus(net_a, vn_kv=swing_kv, name='Ex_Grid_Bus', max_vm_pu=sw_vmax_a, min_vm_pu=sw_vmin_a)
    tie_idx = pp.create_line_from_parameters(net_a, swingbus, ext_grid_idx, 1.0, 0.0, 0.001, 0.0, ext_tie_rating, name='Swing-Tie', max_loading_percent=100.0)
    pp.create_ext_grid(net_a, ext_grid_idx, vm_pu=swing_vreg, va_degree=swing_angle, max_p_mw=1e-3, min_p_mw=-1e-3, max_q_mvar=1e-3, min_q_mvar=-1e-3,
                       s_sc_max_mva=1.0, s_sc_min_mva=1.0, rx_max=0.011, rx_min=0.01, index=ext_grid_idx)
    pp.create_poly_cost(net_a, ext_grid_idx, 'ext_grid', cp1_eur_per_mw=0, cp0_eur=1e9, type='p')
    # pp.create_poly_cost(net_a, ext_grid_idx, 'ext_grid', cq1_eur_per_mvar=1, cq0_eur=1e6, type='q')

    # -- CREATE CONTINGENCY NETWORK EXTERNAL GRID -------------------------------------------------
    pp.create_bus(net_c, vn_kv=swing_kv, name='Ex_Grid_Bus', max_vm_pu=sw_vmax_c, min_vm_pu=sw_vmin_c, index=ext_grid_idx)
    tie_idx = pp.create_line_from_parameters(net_c, swingbus, ext_grid_idx, 1.0, 0.0, 0.001, 0.0, ext_tie_rating, name='Swing-Tie', max_loading_percent=100.0)
    pp.create_ext_grid(net_c, ext_grid_idx, vm_pu=swing_vreg, va_degree=swing_angle, max_p_mw=1e-3, min_p_mw=-1e-3, max_q_mvar=1e-3, min_q_mvar=-1e-3,
                       s_sc_max_mva=1.0, s_sc_min_mva=1.0, rx_max=0.01, rx_min=0.01, index=ext_grid_idx)
    pp.create_poly_cost(net_c, ext_grid_idx, 'ext_grid', cp1_eur_per_mw=0, cp0_eur=1e9, cq1_eur_per_mvar=0, cq0_eur=1e9)
    # pp.create_poly_cost(net_c, ext_grid_idx, 'ext_grid', cq1_eur_per_mvar=0, cq0_eur=1e9, type='q')

    print('   NETWORKS CREATED ................................................', round(time.time() - create_starttime, 3))

    # == MISC NETWORK DATA ========================================================================
    # swsh_q_mins = {}
    # swsh_q_maxs = {}
    # for swshidx in swshidxs:
    #     swsh_q_min = net_a.gen.loc[swshidx, 'min_q_mvar']
    #     swsh_q_max = net_a.gen.loc[swshidx, 'max_q_mvar']
    #     swsh_q_mins.update({swshidx: swsh_q_min})
    #     swsh_q_maxs.update({swshidx: swsh_q_max})
    #
    goutagekeys = outage_dict['gen'].keys()                                                         # GET OUTAGED GENERATOR KEYS
    boutagekeys = outage_dict['branch'].keys()                                                      # GET OUTAGED BRANCH KEYS

    online_gens = []                                                                                # INITIALIZE ONLINE GENERATOR LIST
    for gkey in gen_dict:                                                                           # LOOP ACROSS GENERATOR KEYS
        gidx = gen_dict[gkey]                                                                       # GET GENERATOR INDEX
        if not net_a.gen.loc[gidx, 'in_service']:                                                   # CHECK IF GENERATOR IS ONLINE
            continue                                                                                # IF NOT ONLINE, GET NEXT GENERATOR KEY
        online_gens.append(gkey)                                                                    # ADD ONLINE GENERATOR TO LIST

    external_pgen_threshold = 1e-4
    external_qgen_threshold = 1e-4
    swing_qmin = net_a.gen.loc[swinggen_idxs[0], 'min_q_mvar']
    swing_qmax = net_a.gen.loc[swinggen_idxs[0], 'max_q_mvar']

    levels_out = 3                                                                                  # DEFINE NLEVELS
    nlevel_buses = [swingbus]                                                                       # INITIALIZE BUS LIST
    for i in range(levels_out):                                                                     # LOOP THROUGH HOW MANY LEVELS...
        nlevel_buses = pp.get_connected_buses(net_a, nlevel_buses, respect_in_service=True)         # NEW BUSES ARE ADDED TO BUSLIST
    nlevel_buses = [x for x in nlevel_buses if x != ext_grid_idx]                                   # REMOVE EXTGRID BUS FROM BUSLIST
    # =============================================================================================

    # -- SOLVE INITIAL NETWORKS WITH STRAIGHT POWERFLOW -------------------------------------------
    solve_starttime = time.time()
    pp.runpp(net_a, enforce_q_lims=True)                                                            # SOLVE INITIAL BASE NETWORK
    pp.runpp(net_c, enforce_q_lims=True)                                                            # SOLVE INITIAL CONTINGENCY NETWORK
    print('   NETWORKS SOLVED .................................................', round(time.time() - solve_starttime, 3))

    # ---------------------------------------------------------------------------------------------
    # -- CREATE INITIAL BASECASE OPF --------------------------------------------------------------
    # ---------------------------------------------------------------------------------------------
    print('------------------ CREATING INITIAL BASECASE OPF -------------------')
    opf_startime = time.time()
    pp.runopp(net_a, enforce_q_lims=True)                                                           # RUN OPF ON THIS NETWORK
    net_c.gen['p_mw'] = net_a.res_gen['p_mw']                                                       # SET THIS NETWORK GENERATORS POWER TO OPF GENERATORS POWER (WITH OUTAGE)
    for genbus in genidx_dict:                                                                      # LOOP ACROSS GENERATOR BUSES
        if genbus == swingbus:                                                                      # CHECK IF SWING BUS...
            continue                                                                                # IF SWING BUS, GET NEXT GENERATOR
        gidx = genidx_dict[genbus]                                                                  # GET GENERATOR INDEX
        net_a.gen.loc[gidx, 'vm_pu'] = net_a.res_bus.loc[genbus, 'vm_pu']                           # SET THIS NETWORK GENS VREG TO OPF RESULTS
        net_c.gen.loc[gidx, 'vm_pu'] = net_a.res_bus.loc[genbus, 'vm_pu']                           # SET THIS NETWORK GENS VREG TO OPF RESULTS
    for shbus in swshidx_dict:                                                                      # LOOP ACROSS SWSHUNT (GEN) BUSES
        shidx = swshidx_dict[shbus]                                                                 # GET SWSHUNT INDEX
        net_a.gen.loc[shidx, 'vm_pu'] = net_a.res_bus.loc[shbus, 'vm_pu']                           # SET THIS NETWORK SWSHUNT VREG TO OPF RESULTS
        net_c.gen.loc[shidx, 'vm_pu'] = net_a.res_bus.loc[shbus, 'vm_pu']                           # SET THIS NETWORK SWSHUNT VREG TO OPF RESULTS
    nlevel_buses_v = [x for x in net_a.res_bus.loc[nlevel_buses, 'vm_pu']]                          # GET LIST OF NLEVEL BUS VOLTAGES
    max_v = max(nlevel_buses_v)
    net_a.gen.loc[swinggen_idxs, 'vm_pu'] = max_v                                                   # SET SWING GENS VREG = MAX OF NLEVEL BUSES VOLTAGE
    net_a.ext_grid.loc[ext_grid_idx, 'vm_pu'] = max_v                                               # SET EXTGRID VREG = MAX OF NLEVEL BUSES VOLTAGE
    net_c.gen.loc[swinggen_idxs, 'vm_pu'] = max_v                                                   # SET SWING GENS VREG = MAX OF NLEVEL BUSES VOLTAGE
    net_c.ext_grid.loc[ext_grid_idx, 'vm_pu'] = max_v                                               # SET EXTGRID VREG = MAX OF NLEVEL BUSES VOLTAGE
    pp.runpp(net_a, enforce_q_lims=True)                                                            # RUN STRAIGHT POWER FLOW ON BASECASE
    pp.runpp(net_c, enforce_q_lims=True)                                                            # RUN STRAIGHT POWER FLOW ON CONTINGENCY BASECASE
    init_cost = get_generation_cost(net_a, gen_dict, pwlcost_dict0)
    print('INITIAL BASECASE OPF CREATED .............. ${0:8.1f} ..............'.format(init_cost), round(time.time() - opf_startime, 3))
    print('INITIAL SWING VREG SETPOINT = {0:.4f} ...............................'.format(max_v))

    # ---------------------------------------------------------------------------------------------
    # -- CHECK FOR SOLVED AND NOT SOLVED OUTAGES WITH STRAIGHT POWER FLOW -------------------------
    # ---------------------------------------------------------------------------------------------
    print('---------------- CHECKING FOR NOSOLVE CONTINGENCIES ----------------')
    ns_net = copy.deepcopy(net_c)                                                                   # GET FRESH COPY OF CONTINGENCY BASECASE NETWORK
    solved_outages = []                                                                             # INITIALIZE SOLVED OUTAGES LIST
    nosolve_outages0 = []                                                                           # INITIALIZE NOSOLVE OUTAGES LIST
    # -- CHECK CONTINGENCIES FOR NO SOLUTION ----------------------------------
    ns_text = ''                                                                                    # INITIALIZE OUTAGE TEXT
    for gkey in goutagekeys:                                                                        # LOOP THROUGH GENERATOR OUTAGES
        net = copy.deepcopy(ns_net)                                                                 # GET FRESH COPY OF NOSOLVE NETWORK
        ns_text = 'GEN '                                                                            # ASSIGN TEXT
        gidx = gen_dict[gkey]                                                                       # GET GENERATOR INDEX
        net.gen.in_service[gidx] = False                                                            # SWITCH OFF OUTAGED GENERATOR
        try:                                                                                        # TRY STRAIGHT POWERFLOW SOLUTION
            pp.runpp(net, enforce_q_lims=True)                                                      # RUN POWERFLOW
            solved_outages.append(gkey)                                                             # APPEND SOLVED OUTAGES LIST
        except:                                                                                     # IF NO SOLUTION...
            nosolve_outages0.append(gkey)                                                                   # APPEND NOT SOLVED OUTAGES LIST
            print(ns_text, '{0:9s} Not Solved with Initial OPF Basecase ................'.format(gkey))     # PRINT INFO ON OUTAGE
    for bkey in boutagekeys:                                                                                # LOOP THROUGH BRANCH OUTAGES
        net = copy.deepcopy(ns_net)                                                                 # GET FRESH COPY OF NOSOLVE NETWORK
        if bkey in line_dict:                                                                       # CHECK IF BRANCH IS A LINE...
            ns_text = 'LINE'                                                                        # ASSIGN TEXT
            lineidx = line_dict[bkey]                                                               # GET LINE INDEX
            net.line.in_service[lineidx] = False                                                    # SWITCH OUT OUTAGED LINE
        elif bkey in xfmr_dict:                                                                     # CHECK IF BRANCH IS A XFMR...
            ns_text = 'XFMR'                                                                        # ASSIGN TEXT
            xfmridx = xfmr_dict[bkey]                                                               # GET XFMR INDEX
            net.trafo.in_service[xfmridx] = False                                                   # SWITCH OUT OUTAGED XFMR
        try:                                                                                        # TRY STRAIGHT POWERFLOW SOLUTION
            pp.runpp(net, init='results', enforce_q_lims=True)                                      # RUN POWERFLOW
            solved_outages.append(bkey)                                                             # APPEND NO SOLVE OUTAGES LIST
        except:                                                                                     # IF NO SOLUTION...
            nosolve_outages0.append(bkey)                                                                   # APPEND NOT SOLVED OUTAGES LIST
            print(ns_text, '{0:9s} Not Solved with Initial OPF Basecase ................'.format(bkey))     # PRINT INFO ON OUTAGE

    # ---------------------------------------------------------------------------------------------
    # -- FIND OUTAGES RESULTING IN LOADING CONSTRAINTS --------------------------------------------
    # -- THEN ITERATE WITH OPF TO FINE OPTIMAL BASECASE OPERATING POINT ---------------------------
    # ---------------------------------------------------------------------------------------------
    print('------------- FINDING OPTIMAL BASECASE OPERATING POINT -------------')
    scopf_startime = time.time()
    # -- FIND SOLVED OUTAGES WITH LOADING CONSTRAINTS ---------------------------------------------
    scopf_net = copy.deepcopy(net_c)                                                                # GET FRESH COPY OF CONTINGENCY BASECASE NETWORK
    constrained_outages = []                                                                        # INITIALIZE CONSTRAINED OUTAGES LIST
    constraint = False                                                                              # INITIALIZE CONTRAINT FLAG
    scopf_text = ''                                                                                 # INITIALIZE OUTAGE TEXT
    for scopf_key in solved_outages:                                                                # LOOP ACROSS SOLVED CONTINGENCIES
        net = copy.deepcopy(scopf_net)                                                              # GET FRESH COPY OF NOSOLVE NETWORK
        if scopf_key in online_gens:                                                                # CHECK IF A GENERATOR...
            scopf_text = 'GEN '                                                                     # ASSIGN TEXT
            gidx = gen_dict[scopf_key]                                                              # GET GENERATOR INDEX
            net.gen.in_service[gidx] = False                                                        # SWITCH OFF OUTAGED GENERATOR
        elif scopf_key in line_dict:                                                                # CHECK IF A LINE...
            scopf_text = 'LINE'                                                                     # ASSIGN TEXT
            lineidx = line_dict[scopf_key]                                                          # GET LINE INDEX
            net.line.in_service[lineidx] = False                                                    # SWITCH OUT OUTAGED LINE
        elif scopf_key in xfmr_dict:                                                                # CHECK IF A XFMR...
            scopf_text = 'XFMR'                                                                     # ASSIGN TEXT
            xfmridx = xfmr_dict[scopf_key]                                                          # GET XFMR INDEX
            net.trafo.in_service[xfmridx] = False                                                   # SWITCH OUT OUTAGED XFMR
        try:                                                                                        # TRY STRAIGHT POWERFLOW SOLUTION
            pp.runpp(net, enforce_q_lims=True)                                                      # RUN POWERFLOW
            line_loading = max(net.res_line['loading_percent'].values)                              # GET MAX LINE LOADING
            xfmr_loading = max(net.res_trafo['loading_percent'].values)                             # GET MAX LINE LOADING
            max_loading = max(line_loading, xfmr_loading)                                           # GET MAX OF MAXs
            if max_loading > 100.0:                                                                 # IF MAX LOADING < 100%
                constraint = True                                                                   # SET CONSTRAINT FLAG
                constrained_outages.append([max_loading, scopf_key])                                # ADD OUTAGE TO CONSTRAINED OUTAGES LIST
        except:                                                                                                     # IF NO SOLUTION...
            print(scopf_text, '{0:9s} NOT SOLVED USING INITIAL OPF BASECASE ...............'.format(scopf_key))     # PRINT INFO ON OUTAGE
    constrained_outages.sort(reverse=True)                                                                          # SORT CONSTRAINED OUTAGES LIST
    # constrained_outages.sort()                                                                    # SORT CONSTRAINED OUTAGES LIST
    constrained_outages = [x[1] for x in constrained_outages]                                       # GET ONLY KEYS OF CONTRAINED OUTAGES

    # -- ITERATE WITH OPF TO MINIMIZE CONSTRAINED OUTAGES -----------------------------------------
    step = 1                                                                                        # INITIALIZE ITERATOR
    scopf_net = copy.deepcopy(net_c)                                                                # GET FRESH COPY OF CONTINGENCY BASECASE NETWORK
    while constraint and step < 2:                                                                  # LOOP WHILE CONSTRAINT EXISTS
        contraint = False                                                                           # SET CONSTRAINT FLAG
        for scopf_key in constrained_outages:                                                       # LOOP THROUGH CONSTRAINED OUTAGES
            constrained_outages1 = []                                                               # INITIALIZE A 'SUB' CONTRAINED OUTAGES LIST
            net = copy.deepcopy(scopf_net)                                                          # GET FRESH COPY OF SCOPF NETWORK
            if scopf_key in online_gens:                                                            # CHECK IF A GENERATOR...
                scopf_text = 'GEN '                                                                 # ASSIGN TEXT
                gidx = gen_dict[scopf_key]                                                          # GET GENERATOR INDEX
                net.gen.in_service[gidx] = False                                                    # SWITCH OFF OUTAGED GENERATOR
            elif scopf_key in line_dict:                                                            # CHECK IF A LINE...
                scopf_text = 'LINE'                                                                 # ASSIGN TEXT
                lineidx = line_dict[scopf_key]                                                      # GET LINE INDEX
                net.line.in_service[lineidx] = False                                                # SWITCH OUT OUTAGED LINE
            elif scopf_key in xfmr_dict:                                                            # CHECK IF A XFMR...
                scopf_text = 'XFMR'                                                                 # ASSIGN TEXT
                xfmridx = xfmr_dict[scopf_key]                                                      # GET XFMR INDEX
                net.trafo.in_service[xfmridx] = False                                               # SWITCH OUT OUTAGED XFMR
            pp.runpp(net, enforce_q_lims=True)                                                      # RUN POWERFLOW ON OUTAGE
            line_loading = max(net.res_line['loading_percent'].values)                              # GET MAX LINE LOADING
            xfmr_loading = max(net.res_trafo['loading_percent'].values)                             # GET MAX LINE LOADING
            max_loading = max(line_loading, xfmr_loading)                                           # GET MAX OF MAXs
            if max_loading > 100.0:                                                                                                 # IF MAX LOADING < 100%
                print(scopf_text, '{0:9s} Outage Results in Loading Constraint ({1:.1f}%) .......'.format(scopf_key, max_loading))  # PRINT INFO ON OUTAGE
                constraint = True                                                                                                   # SET CONSTRAINT FLAG
                constrained_outages1.append(scopf_key)                                              # ADD OUTAGE TO 'SUB CONSTRAINED OUTAGES LIST
                pp.runopp(net, enforce_q_lims=True)                                                 # RUN OPF ON THIS NETWORK
                scopf_net.gen['p_mw'] = net.res_gen['p_mw']                                         # SET THIS NETWORK GENERATORS POWER TO OPF RESULTS
                net_a.gen['p_mw'] = net.res_gen['p_mw']                                             # SET THIS NETWORK GENERATORS POWER TO OPF RESULTS
                net_c.gen['p_mw'] = net.res_gen['p_mw']                                             # SET THIS NETWORK GENERATORS POWER TO OPF RESULTS
                for genbus in genidx_dict:                                                          # LOOP ACROSS GENERATOR BUSES
                    if genbus == swingbus:                                                          # CHECK IF SWING SWING BUS...
                        continue                                                                    # IF SWING BUS, GET NEXT GENERATOR
                    gidx = genidx_dict[genbus]                                                      # GET GENERATOR INDEX
                    scopf_net.gen.loc[gidx, 'vm_pu'] = net.res_bus.loc[genbus, 'vm_pu']             # SET THIS NETWORK GENS VREG TO TO OPF RESULTS
                    net_a.gen.loc[gidx, 'vm_pu'] = net.res_bus.loc[genbus, 'vm_pu']                 # SET THIS NETWORK GENS VREG TO TO OPF RESULTS
                    net_c.gen.loc[gidx, 'vm_pu'] = net.res_bus.loc[genbus, 'vm_pu']                 # SET THIS NETWORK GENS VREG TO TO OPF RESULTS
                for shbus in swshidx_dict:                                                          # LOOP ACROSS SWSHUNT (GEN) BUSES
                    shidx = swshidx_dict[shbus]                                                     # GET SWSHUNT INDEX
                    scopf_net.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']             # SET THIS NETWORK SWSHUNT VREG TO OPF RESULTS
                    net_a.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']                 # SET THIS NETWORK SWSHUNT VREG TO OPF RESULTS
                    net_c.gen.loc[shidx, 'vm_pu'] = net.res_bus.loc[shbus, 'vm_pu']                 # SET THIS NETWORK SWSHUNT VREG TO OPF RESULTS
            pp.runpp(scopf_net, enforce_q_lims=True)                                                # RUN STRAIGHT POWER FLOW ON SCOPF NETWORK
            # pp.runpp(net_a, enforce_q_lims=True)                                                    # RUN STRAIGHT POWER FLOW ON BASECASE NETWORK
            # pp.runpp(net_c, enforce_q_lims=True)                                                    # RUN STRAIGHT POWER FLOW ON CONTINGENCY BASECASE NETWORK
        constrained_outages = constrained_outages1                                                  # REDUCE NEXT ITERATION CONSTRAINED OUTAGES
        step += 1                                                                                   # INCREMENT ITERATOR
    pp.runpp(net_c, enforce_q_lims=True)                                                            # RUN STRAIGHT POWER FLOW ON CONTINGENCY BASECASE
    pp.runpp(net_a, enforce_q_lims=True)                                                            # RUN STRAIGHT POWER FLOW ON BASECASE
    external_pgen = net_a.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                    # GET EXTERNAL GRID REAL POWER
    external_qgen = net_a.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                  # GET EXTERNAL GRID REACTIVE POWER
    print('--------------------------------------------------------------------')

    # ---------------------------------------------------------------------------------------------
    # -- INSURE MVAR MARGIN ON SWING GENERATORS ---------------------------------------------------
    # ---------------------------------------------------------------------------------------------
    net = copy.deepcopy(net_a)                                                                      # GET FRESH COPY OF BASECASE NETWORK
    sw_qmin = 0.90 * swing_qmin                                                                     # DEFINE SWING GENERATORS QMIN FOR 10% MARGIN
    sw_qmax = 0.90 * swing_qmin                                                                     # DEFINE SWING GENERATORS QMAX FOR 10% MARGIN
    swing_q = net.res_gen.loc[swinggen_idxs[0], 'q_mvar']                                           # GET SWING GENERATORS REACTIVE POWER
    sw_vreg = net.res_gen.loc[swinggen_idxs[0], 'vm_pu']                                            # GET SWING GENERATORS VOLTAGE SETPOINT

    if swing_q < sw_qmin:                                                                           # CHECK IF SWING MVAR IS LESS THAN THRESHOLD
        step = 1                                                                                    # INITIALIZE ITERATOR
        while swing_q < sw_qmin and step < 40:                                                      # LOOP WHILE SWING MVAR IS LESS THAN THRESHOLD
            sw_vreg += 0.0005                                                                       # INCREASE SWING GENERATORS VOLTAGE SETPOINT
            sw_vreg = min(sw_vreg, sw_vmax_a - 0.0001)                                              # INSURE SETPOINT IS LESS THAN MAX BUS VOLTAGE
            net.gen.loc[swinggen_idxs, 'vm_pu'] = sw_vreg                                           # THIS NETWORK, SET SWING GENERATORS VOLTAGE SETPOINT
            net.ext_grid.loc[ext_grid_idx, 'vm_pu'] = sw_vreg                                       # THIS NETWORK, SET EXTERNAL GRID VOLTAGE SETPOINT
            net_a.gen.loc[swinggen_idxs, 'vm_pu'] = sw_vreg                                         # THIS NETWORK, SET SWING GENERATORS VOLTAGE SETPOINT
            net_a.ext_grid.loc[ext_grid_idx, 'vm_pu'] = sw_vreg                                     # THIS NETWORK, SET EXTERNAL GRID VOLTAGE SETPOINT
            net_c.gen.loc[swinggen_idxs, 'vm_pu'] = sw_vreg                                         # THIS NETWORK, SET SWING GENERATORS VOLTAGE SETPOINT
            net_c.ext_grid.loc[ext_grid_idx, 'vm_pu'] = sw_vreg                                     # THIS NETWORK, SET EXTERNAL GRID VOLTAGE SETPOINT
            pp.runpp(net, enforce_q_lims=True)                                                      # THIS NETWORK, RUN STRAIGHT POWER
            swing_q = net.res_gen.loc[swinggen_idxs[0], 'q_mvar']                                   # GET SWING GENERATORS NEW REACTIVE POWER
            step += 1                                                                               # INCREMENT ITERATOR
        pp.runpp(net_a, enforce_q_lims=True)                                                        # RUN STRAIGHT POWER FLOW ON BASECASE
        pp.runpp(net_c, enforce_q_lims=True)                                                        # RUN STRAIGHT POWER FLOW ON CONTINGENCY BASECASE

    elif swing_q > sw_qmax:                                                                         # CHECK IF SWING MVAR IS MORE THAN THRESHOLD
        step = 1                                                                                    # INITIALIZE ITERATOR
        while swing_q > sw_qmax and step < 40:                                                      # LOOP WHILE SWING MVAR IS MORE THAN THRESHOLD
            sw_vreg -= 0.0005                                                                       # DECREASE SWING GENERATORS VOLTAGE SETPOINT
            sw_vreg = min(sw_vreg, sw_vmin_a + 0.0001)                                              # INSURE SETPOINT IS MORE THAN MIN BUS VOLTAGE
            net.gen.loc[swinggen_idxs, 'vm_pu'] = sw_vreg                                           # THIS NETWORK, SET SWING GENERATORS VOLTAGE SETPOINT
            net.ext_grid.loc[ext_grid_idx, 'vm_pu'] = sw_vreg                                       # THIS NETWORK, SET EXTERNAL GRID VOLTAGE SETPOINT
            net_a.gen.loc[swinggen_idxs, 'vm_pu'] = sw_vreg                                         # THIS NETWORK, SET SWING GENERATORS VOLTAGE SETPOINT
            net_a.ext_grid.loc[ext_grid_idx, 'vm_pu'] = sw_vreg                                     # THIS NETWORK, SET EXTERNAL GRID VOLTAGE SETPOINT
            net_c.gen.loc[swinggen_idxs, 'vm_pu'] = sw_vreg                                         # THIS NETWORK, SET SWING GENERATORS VOLTAGE SETPOINT
            net_c.ext_grid.loc[ext_grid_idx, 'vm_pu'] = sw_vreg                                     # THIS NETWORK, SET EXTERNAL GRID VOLTAGE SETPOINT
            pp.runpp(net, enforce_q_lims=True)                                                      # THIS NETWORK, RUN STRAIGHT POWER
            swing_q = net.res_gen.loc[swinggen_idxs[0], 'q_mvar']                                   # GET SWING GENERATORS NEW REACTIVE POWER
            step += 1                                                                               # INCREMENT ITERATOR
        pp.runpp(net_a, enforce_q_lims=True)                                                        # RUN STRAIGHT POWER FLOW ON BASECASE
        pp.runpp(net_c, enforce_q_lims=True)                                                        # RUN STRAIGHT POWER FLOW ON CONTINGENCY BASECASE

    # ---------------------------------------------------------------------------------------------
    # -- ZERO OUT EXTERNAL GRID REAL AND REACTIVE POWER CONTRIBUTIONS -----------------------------
    # ---------------------------------------------------------------------------------------------
    net = copy.deepcopy(net_a)
    zeroed = abs(external_pgen) < external_pgen_threshold or abs(external_qgen) < external_qgen_threshold
    step = 1
    while not zeroed and step < 120:
        zeroed = True
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
                    delta_vreg = 0.020 * external_qgen * q_downmargin_dict[gidx] / q_downmargin_total   # CALCULATE SETPOINT INCREMENT (DISTRIBUTED PROPORTIONALLY)
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
    base_cost = get_generation_cost(net_a, gen_dict, pwlcost_dict0)                                 # GET TOTAL COST OF GENERATION
    print('BASECASE OPF CREATED .................. ${0:8.1f} ..................'.format(base_cost), round(time.time() - scopf_startime, 3))
    print('SWING VREG SETPOINT = {0:.4f} .......................................'.format(sw_vreg))
    # ---------------------------------------------------------------------------------------------
    # -- CURIOSITY CHECK IF NOSOLVE OUTAGES NOW SOLVE WITH THIS BASECASE OPF ----------------------
    # ---------------------------------------------------------------------------------------------
    ns_net = copy.deepcopy(net_a)                                                                   # GET FRESH COPY OF BASECASE NETWORK
    ns_text = ''                                                                                    # INITIALIZE OUTAGE TEXT
    for nskey in nosolve_outages0:                                                                  # LOOP ACROSS SOLVED KEYS
        net = copy.deepcopy(ns_net)                                                                 # GET FRESH COPY OF NOSOLVE NETWORK
        if nskey in online_gens:                                                                    # CHECK IF A GENERATOR...
            ns_text = 'GEN '                                                                        # ASSIGN TEXT
            gidx = gen_dict[nskey]                                                                  # GET GENERATOR INDEX
            net.gen.in_service[gidx] = False                                                        # SWITCH OFF OUTAGED GENERATOR
        elif nskey in line_dict:                                                                    # CHECK IF A LINE...
            ns_text = 'LINE'                                                                        # ASSIGN TEXT
            lineidx = line_dict[nskey]                                                              # GET LINE INDEX
            net.line.in_service[lineidx] = False                                                    # SWITCH OUT OUTAGED LINE
        elif nskey in xfmr_dict:                                                                    # CHECK IF A XFMR...
            ns_text = 'XFMR'                                                                        # ASSIGN TEXT
            xfmridx = xfmr_dict[nskey]                                                              # GET XFMR INDEX
            net.trafo.in_service[xfmridx] = False                                                   # SWITCH OUT OUTAGED XFMR
        try:                                                                                        # TRY STRAIGHT POWERFLOW SOLUTION
            pp.runpp(net, enforce_q_lims=True)                                                      # RUN POWERFLOW
        except:                                                                                             # IF NO SOLUTION...
            print(ns_text, '{0:9s} STILL NOT SOLVED USING BASECASE OPF .................'.format(nskey))    # PRINT INFO ON OUTAGE

    pp.runpp(net_a, enforce_q_lims=True)                                                            # RUN STRAIGHT POWER FLOW ON BASECASE
    ex_pgen = net_a.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                          # GET EXTERNAL GRID REAL POWER
    ex_qgen = net_a.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                        # GET EXTERNAL GRID REACTIVE POWER

    # -- GET BASECASE GENERATORS POWER OUTPUT -----------------------------------------------------
    base_pgens = {}                                                                                 # INITIALIZE BASECASE GENERATOR POWER DICT
    for gkey in online_gens:                                                                        # LOOP ACROSS GENERATOR KEYS
        gidx = gen_dict[gkey]                                                                       # GET GENERATOR INDEX
        base_pgen = net_a.res_gen.loc[gidx, 'p_mw']                                                 # GET THIS GENERATOR POWER OUTPUT
        base_pgens.update({gkey: base_pgen})                                                        # UPDATE BASECASE GENERATOR POWER DICT

    # ---------------------------------------------------------------------------------------------
    # -- WRITE BASECASE BUS AND GENERATOR RESULTS TO FILE -----------------------------------------
    # ---------------------------------------------------------------------------------------------
    print('WRITING BASECASE RESULTS TO FILE ..... {0:.5f} MW {1:.5f} MVAR .....'.format(ex_pgen, ex_qgen))
    bus_results = copy.deepcopy(net_a.res_bus)                                                      # GET BASECASE BUS RESULTS
    gen_results = copy.deepcopy(net_a.res_gen)                                                      # GET BASECASE GENERATOR RESULTS
    write_base_bus_results(outfname1, bus_results, swshidx_dict, gen_results, ext_grid_idx)         # WRITE SOLUTION1 BUS RESULTS
    write_base_gen_results(outfname1, gen_results, gids, genbuses, swshidxs)                        # WRITE SOLUTION1 GEN RESULTS

    # ---------------------------------------------------------------------------------------------
    # == WRITE DATA TO FILE =======================================================================
    # ---------------------------------------------------------------------------------------------
    print('====================================================================')
    write_starttime = time.time()
    # -- WRITE RATEA NETWORK TO FILE ----------------------
    pp.to_pickle(net_a, neta_fname)
    # -- WRITE RATEC NETWORK TO FILE ----------------------
    pp.to_pickle(net_c, netc_fname)
    # -- WRITE DATA TO FILE -------------------------------
    PFile = open(data_fname, 'wb')
    pickle.dump([outage_dict, gen_dict, xfmr_dict, pfactor_dict, ext_grid_idx, gids, genbuses, swshidxs, swshidx_dict,
                 line_dict, genidx_dict, swinggen_idxs, online_gens, base_pgens, participating_gens], PFile)
    PFile.close()
    print('WRITING DATA TO FILE -----------------------------------------------', round(time.time() - write_starttime, 3))
    print('DONE ---------------------------------------------------------------')
    print('TOTAL TIME -------------------------------------------------------->', round(time.time() - start_time, 3))



    print()
    print()
    print()
    print('TESTING CALCULATING DELTA VARIABLES --------------------------------')
    # -- TODO run outages and get delta in Mypython2 ----------------------------------------------
    # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
    # -- RUN GENERATOR OUTAGES TO FIND OPTIMUM DELTA VARIABLE -------------------------------------
    # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
    gdelta_starttime = time.time()
    # cnet = copy.deepcopy(net_c)                                                                   # GET FRESH COPY OF CONTINGENCY NETWORK
    # for genbus in genidx_dict:                                                                    # LOOP ACROSS GENERATOR BUSES
    #     gidx = genidx_dict[genbus]                                                                # GET GENERATOR INDEX
    #     cnet.gen.loc[gidx, 'controllable'] = False                                                # TURN OFF GENERATOR OPF CONTROL (NOT SWSHUNTS)
    #
    # # -- TODO DOES OPTIMIZE SHUNTS GO BEFORE LAST DELTA ITERATION?
    # pp.runopp(cnet, enforce_q_lims=True)                                                          # RUN OPTIMAL POWER FLOW (OPTIMIZE SWSHUNTS)
    # for shbus in swshidx_dict:                                                                    # LOOP ACROSS SWSHUNT (GEN) BUSES
    #     shidx = swshidx_dict[shbus]                                                               # GET SWSHUNT INDEX
    #     cnet.gen.loc[shidx, 'vm_pu'] = cnet.res_bus.loc[shbus, 'vm_pu']                           # SET SWSHUNT VREG TO OPF RESULTS
    # pp.runpp(cnet, enforce_q_lims=True)                                                           # RUN STRAIGHT POWER FLOW

    for genkey in goutagekeys:                                                                      # LOOP THROUGH GENERATOR OUTAGES
        gnet = copy.deepcopy(net_c)                                                                 # GET FRESH COPY OF CONTINGENCY NETWORK
        if genkey not in online_gens:
            print('genkey not in online gens')
            continue                                                                                # IF OFFLINE, GET NEXT GENERATOR
        genidx = gen_dict[genkey]                                                                   # GET OUTAGED GENERATOR INDEX
        conlabel = "'" + outage_dict['gen'][genkey] + "'"                                           # GET CONTINGENCY LABEL
        gen_outage_p = base_pgens[genkey]                                                           # GET OUTAGED GENERATOR PGEN
        gnet.gen.in_service[genidx] = False                                                         # SWITCH OFF OUTAGED GENERATOR

        # -- FIRST ESTIMATE OF DELTA VARIABLE -----------------------------------------------------
        pfactor_total = 0.0                                                                         # INITIALIZE PARTICIPATION FACTORS TOTAL
        for gen_pkey in participating_gens:                                                         # LOOP ACROSS PARTICIPATING GENERATORS
            if gen_pkey == genkey:                                                                  # DON'T WANT PFACTOR FOR OUTAGED GENERATOR
                continue                                                                            # GET NEXT PARTICIPATING GENERATOR
            gidx = gen_dict[gen_pkey]                                                               # GET PARTICIPATING GENERATOR INDEX
            pfactor_total += pfactor_dict[gen_pkey]                                                 # INCREMENT PFACTOR TOTAL
        pp.runpp(gnet, enforce_q_lims=True)                                                         # RUN STRAIGHT POWER FLOW
        ex_pgen0 = gnet.res_ext_grid.loc[ext_grid_idx, 'p_mw']
        delta_init = (gen_outage_p + ex_pgen0) / pfactor_total                                      # INITIAL DELTA VARIABLE ESTIMATE
        # delta_init = ex_pgen0 / (pfactor_total * (1 - math.exp(-1)))                              # INITIAL DELTA VARIABLE ESTIMATE
        # delta_init = (gen_outage_p + ex_pgen0) / (pfactor_total * (1 - math.exp(-1)))             # INITIAL DELTA VARIABLE ESTIMATE

        # -- ITERATE TO FIND OPTIMUM GENERATOR OUTAGE DELTA VARIABLE  ------------------------------
        step = 1                                                                                    # INITIALIZE ITERATOR
        delta = delta_init                                                                          # SET INITIAL DELTA VARIABL
        net = copy.deepcopy(gnet)                                                                   # GET COPY OF OUTAGED NETWORK
        while step < 120:                                                                           # LIMIT WHILE LOOPS
            for gen_pkey in participating_gens:                                                     # LOOP THROUGH PARTICIPATING GENERATORS
                if gen_pkey == genkey:                                                              # IF GENERATOR = OUTAGED GENERATOR
                    continue                                                                        # GET NEXT GENERATOR
                gidx = gen_dict[gen_pkey]                                                           # GET THIS GENERATOR INDEX
                pgen_a = base_pgens[gen_pkey]                                                       # THIS GENERATORS RATEA BASECASE PGEN
                pmin = net.gen.loc[gidx, 'min_p_mw']                                                # THIS GENERATORS PMIN
                pmax = net.gen.loc[gidx, 'max_p_mw']                                                # THIS GENERATORS MAX
                pfactor = pfactor_dict[gen_pkey]                                                    # THIS GENERATORS PARTICIPATION FACTOR
                target_pgen = pgen_a + pfactor * delta                                              # CALCULATE THIS GENERATORS EXPECTED PGEN
                if pmin < target_pgen < pmax:                                                       # IF EXPECTED PGEN IS IN BOUNDS...
                    net.gen.loc[gidx, 'p_mw'] = target_pgen                                         # SET PGEN = EXPECTED PGEN
                elif target_pgen > pmax:                                                            # IF EXPECTED PGEN > PMAX...
                    net.gen.loc[gidx, 'p_mw'] = pmax                                                # SET PGEN = PMAX
                elif target_pgen < pmin:                                                            # IF EXPECTED PGEN < PMIN...
                    net.gen.loc[gidx, 'p_mw'] = pmin                                                # SET PGEN = PMIN
            pp.runpp(net, enforce_q_lims=True)                                                      # RUN STRAIGHT POWER FLOW
            ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                    # GET EXTERNAL GRID POWER
            if abs(ex_pgen) < 1e-3:                                                                 # IF EXTERNAL GRID POWER IS NEAR ZERO..
                break                                                                               # BREAK AND GET NEXT GEN OUTAGE
            delta += ex_pgen / pfactor_total                                                        # INCREMENT DELTA
            step += 1                                                                               # INCREMENT ITERATION
        ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                        # GET EXTERNAL GRID REAL POWER
        ex_qgen = net.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                      # GET EXTERNAL GRID REACTIVE POWER
        print('GEN {0:5s}  . . . . . . . . . . . . . . . . . . . . . . . . . . . . .'.format(genkey),
              '\u0394 =', round(delta, 6), '(' + str(step) + ')', round(ex_pgen, 6), round(ex_qgen, 6))

        # ---------------------------------------------------------------------------------------------
        # -- WRITE GENERTATOR CONTINGENCY BUS AND GENERATOR RESULTS TO FILE ---------------------------
        # ---------------------------------------------------------------------------------------------
        bus_results = copy.deepcopy(net.res_bus)                                                        # GET CONTINGENCY BUS RESULTS
        gen_results = copy.deepcopy(net.res_gen)                                                        # GET CONTINGENCY GENERATOR RESULTS
        write_bus_results(outfname2, bus_results, swshidx_dict, gen_results, ext_grid_idx, conlabel)    # WRITE SOLUTION2 BUS RESULTS
        write_gen_results(outfname2, gen_results, gids, genbuses, delta, swshidxs)                      # WRITE SOLUTION2 GENERATOR RESULTS
    print('DELTAS FOR GENERATOR OUTAGES ESTIMATED .............................', round(time.time() - gdelta_starttime, 3))
    print()

    # -- TODO run outages and get delta in Mypython2 ----------------------------------------------
    # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
    # -- RUN LINE AND XFMR OUTAGES TO FIND OPTIMUM DELTA VARIABLE ---------------------------------
    # =+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=
    bdelta_starttime = time.time()
    # cnet = copy.deepcopy(net_c)                                                                   # GET FRESH COPY OF CONTINGENCY NETWORK
    # for genbus in genidx_dict:                                                                    # LOOP ACROSS GENERATOR BUSES
    #     gidx = genidx_dict[genbus]                                                                # GET GENERATOR INDEX
    #     cnet.gen.loc[gidx, 'controllable'] = False                                                # TURN OFF GENERATOR OPF CONTROL (NOT SWSHUNTS)
    #
    # # -- TODO DOES OPTIMIZE SHUNTS GO BEFORE LAST DELTA ITERATION?
    # pp.runopp(cnet, enforce_q_lims=True)                                                          # RUN OPTIMAL POWER FLOW (OPTIMIZE SWSHUNTS)
    # for shbus in swshidx_dict:                                                                    # LOOP ACROSS SWSHUNT (GEN) BUSES
    #     shidx = swshidx_dict[shbus]                                                               # GET SWSHUNT INDEX
    #     cnet.gen.loc[shidx, 'vm_pu'] = cnet.res_bus.loc[shbus, 'vm_pu']                           # SET SWSHUNT VREG TO OPF RESULTS
    # pp.runpp(cnet, enforce_q_lims=True)                                                           # RUN STRAIGHT POWER FLOW

    pfactor_total = 0.0                                                                             # INITIALIZE PARTICIPATION FACTORS TOTAL
    for gen_pkey in pfactor_dict:                                                                   # LOOP ACROSS PARTICIPATING GENERATORS
        if gen_pkey not in online_gens:                                                             # CHECK IF PARTICIPATING GENERATOR IS ONLINE
            continue                                                                                # IF NOT ONLINE, GET NEXT PARTICIPATING GENERATOR
        gidx = gen_dict[gen_pkey]                                                                   # GET PARTICIPATING GENERATOR INDEX
        pfactor_total += pfactor_dict[gen_pkey]                                                     # INCREMENT PFACTOR TOTAL

    btext = ''                                                                                      # DECLARE BRANCH TYPE TEXT
    for branchkey in boutagekeys:                                                                   # LOOP THROUGH BRANCH OUTAGES
        bnet = copy.deepcopy(net_c)                                                                 # INITIALIZE THIS CONTINGENCY NETWORK
        conlabel = "'" + outage_dict['branch'][branchkey] + "'"                                     # GET CONTINGENCY LABEL
        if branchkey in line_dict:                                                                  # CHECK IF BRANCH IS A LINE...
            btext = 'LINE'                                                                          # ASSIGN BRANCH TYPE TEXT
            lineidx = line_dict[branchkey]                                                          # GET LINE INDEX
            if not bnet.line.loc[lineidx, 'in_service']:                                            # CHECK IF OUTAGED LINE IS IN-SERVICE...
                continue                                                                            # IF NOT, GET NEXT BRANCH
            bnet.line.in_service[lineidx] = False                                                   # TAKE LINE OUT OF SERVICE
        elif branchkey in xfmr_dict:                                                                # CHECK IF BRANCH IS A XFMR...
            btext = 'XFMR'                                                                          # ASSIGN BRANCH TYPE TEXT
            xfmridx = xfmr_dict[branchkey]                                                          # GET XFMR INDEX
            if not bnet.trafo.loc[xfmridx, 'in_service']:                                           # CHECK IF OUTAGED XFMR IS IN-SERVICE...
                continue                                                                            # IF NOT, GET NEXT BRANCH
            bnet.trafo.in_service[xfmridx] = False                                                  # TAKE XFMR OUT OF SERVICE
        try:                                                                                        # TRY STRAIGHT POWER FLOW
            pp.runpp(bnet, enforce_q_lims=True)                                                     # RUN STRAIGHT POWER FLOW
            ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                    # GET EXTERNAL GRID POWER
        except:                                                                                     # IF NOT SOLVED... WRITE BASECASE RESULTS AS PLACEHOLDER
            delta = 0.0                                                                             # ASSIGN DELTA = 0
            bus_results = copy.deepcopy(net_a.res_bus)                                              # GET BASECASE BUS RESULTS
            gen_results = copy.deepcopy(net_a.res_gen)                                                      # GET BASECASE GENERATOR RESULTS
            write_bus_results(outfname2, bus_results, swshidx_dict, gen_results, ext_grid_idx, conlabel)    # WRITE DUMMY BUS RESULTS TO SOLUTION2 FILE
            write_gen_results(outfname2, gen_results, gids, genbuses, delta, swshidxs)                      # WRITE DUMMY BUS RESULTS TO SOLUTION2 FILE
            continue                                                                                # GET NEXT BRANCH

        # -- ITERATE TO FIND OPTIMUM BRANCH OUTAGE DELTA VARIABLE  ---------------------------------
        step = 1                                                                                    # INITIALIZE ITERATOR
        delta = 0.0                                                                                 # INITIALIZE DELTA VARIABLE
        net = copy.deepcopy(bnet)                                                                   # GET FRESH COPY INITIALIZED NETWORK
        while step < 120:                                                                           # LIMIT WHILE LOOPS
            for gen_pkey in participating_gens:                                                     # LOOP THROUGH PARTICIPATING GENERATORS
                gidx = gen_dict[gen_pkey]                                                           # GET THIS GENERATOR INDEX
                pgen_a = base_pgens[gen_pkey]                                                       # THIS GENERATORS RATEA BASECASE PGEN
                pmin = net.gen.loc[gidx, 'min_p_mw']                                                # THIS GENERATORS PMIN
                pmax = net.gen.loc[gidx, 'max_p_mw']                                                # THIS GENERATORS MAX
                pfactor = pfactor_dict[gen_pkey]                                                    # THIS GENERATORS PARTICIPATION FACTOR
                target_pgen = pgen_a + pfactor * delta                                              # CALCULATE THIS GENERATORS EXPECTED PGEN
                if pmin < target_pgen < pmax:                                                       # IF EXPECTED PGEN IS IN BOUNDS...
                    net.gen.loc[gidx, 'p_mw'] = target_pgen                                         # SET PGEN = EXPECTED PGEN
                elif target_pgen > pmax:                                                            # IF EXPECTED PGEN > PMAX...
                    net.gen.loc[gidx, 'p_mw'] = pmax                                                # SET PGEN = PMAX
                elif target_pgen < pmin:                                                            # IF EXPECTED PGEN < PMIN...
                    net.gen.loc[gidx, 'p_mw'] = pmin                                                # SET PGEN = PMIN
            pp.runpp(net, enforce_q_lims=True)                                                      # RUN STRAIGHT POWER FLOW
            ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                    # GET EXTERNAL GRID POWER
            if abs(ex_pgen) < 1e-3:                                                                 # IF EXTERNAL GRID POWER IS NEAR ZERO..
                break                                                                               # BREAK AND GET NEXT GEN OUTAGE
            delta += ex_pgen / pfactor_total                                                        # INCREMENT DELTA
            step += 1                                                                               # INCREMENT ITERATION

        ex_pgen = net.res_ext_grid.loc[ext_grid_idx, 'p_mw']                                        # GET EXTERNAL GRID REAL POWER
        ex_qgen = net.res_ext_grid.loc[ext_grid_idx, 'q_mvar']                                      # GET EXTERNAL GRID REACTIVE POWER
        print('{0:4s} {1:9s} . . . . . . . . . . . . . . . . . . . . . . . . . . . '.format(btext, branchkey),
              '\u0394 =', round(delta, 6), '(' + str(step) + ')', round(ex_pgen, 6), round(ex_qgen, 6))

        # ---------------------------------------------------------------------------------------------
        # -- WRITE BRANCH CONTINGENCY BUS AND GENERATOR RESULTS TO FILE -------------------------------
        # ---------------------------------------------------------------------------------------------
        bus_results = copy.deepcopy(net.res_bus)                                                        # GET CONTINGENCY BUS RESULTS
        gen_results = copy.deepcopy(net.res_gen)                                                        # GET CONTINGENCY GENERATOR RESULTS
        write_bus_results(outfname2, bus_results, swshidx_dict, gen_results, ext_grid_idx, conlabel)    # WRITE SOLUTION2 BUS RESULTS
        write_gen_results(outfname2, gen_results, gids, genbuses, delta, swshidxs)                      # WRITE SOLUTION2 GENERATOR RESULTS
    print('DELTAS FOR LINE AND XFMR OUTAGES ESTIMATED .........................', round(time.time() - bdelta_starttime, 3))
    print()