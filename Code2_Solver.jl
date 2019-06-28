#!/usr/bin/env julia

using Distributed
using SparseArrays
using JSON
using JuMP
using Ipopt
using InfrastructureModels
using PowerModels
using Memento
@everywhere using Pkg
@everywhere Pkg.activate(".")

# -- IDENTIFY CPU THREADS --------------------------------------------------------------------------
max_threads = 12
if Distributed.nprocs() <= 2
    threads = min(trunc(Int, Sys.CPU_THREADS*0.75), max_threads)
    println("threads: $(threads)/$(Sys.CPU_THREADS)")
    proc_ids = Distributed.addprocs(threads)
    println("process ids: $(proc_ids)")
end

# -- WARM UP FOR MULTIPROCESSING -------------------------------------------------------------------
using Pkg
Pkg.activate(".")
# Pkg.instantiate()
# Pkg.status()
time_start = time()
println("start pre-compilation")
using Distributed
using SparseArrays
using JSON
using JuMP
using Ipopt
using InfrastructureModels
using PowerModels
using Memento
println("pre-compilation finished ($(time() - time_start))")

@everywhere using Ipopt
# include("solution2-solver.jl")
# include("parsers.jl")
# include("lib.jl")

# -- CODE2 SOLVER FUNTIONS -------------------------------------------------------------------------
function compute_solution2(con_file::String, inl_file::String, raw_file::String, rop_file::String, time_limit::Int, scoring_method::Int, network_model::String; output_dir::String="", scenario_id::String="none")
    time_data_start = time()
    goc_data = parse_goc_files(con_file, inl_file, raw_file, rop_file, scenario_id=scenario_id)
    network = build_pm_model(goc_data)
    sol = read_solution1(network, output_dir=output_dir)
    PowerModels.update_data!(network, sol)

    check_network_solution(network)

    network_tmp = deepcopy(network)
    balance = compute_power_balance_deltas!(network_tmp)

    if balance.p_delta_abs_max > 0.01 || balance.q_delta_abs_max > 0.01
        error(LOGGER, "solution1 power balance requirements not satified (all power balance values should be below 0.01). $(balance)")
    end

    time_data = time() - time_data_start

    for (i,bus) in network["bus"]
        if haskey(bus, "evhi")
            bus["vmax"] = bus["evhi"]
        end
        if haskey(bus, "evlo")
            bus["vmin"] = bus["evlo"]
        end
    end

    for (i,branch) in network["branch"]
        g, b = PowerModels.calc_branch_y(branch)
        tr, ti = PowerModels.calc_branch_t(branch)
        branch["g"] = g
        branch["b"] = b
        branch["tr"] = tr
        branch["ti"] = ti
    end


    ###### Prepare Solution 2 ######

    time_contingencies_start = time()

    gen_cont_total = length(network["gen_contingencies"])
    branch_cont_total = length(network["branch_contingencies"])

    processes = 1
    if Distributed.nprocs() > 1
        processes = Distributed.nprocs()-1 # save one for the master process
    end

    networks = [copy(network) for n in 1:processes]
    gens_per_proc = trunc(Int, ceil(gen_cont_total/processes))
    branch_per_proc = trunc(Int, ceil(branch_cont_total/processes))

    for p in 1:processes
        network_proc = networks[p]
        network_proc["gen_contingencies"] = network_proc["gen_contingencies"][(1+(p-1)*gens_per_proc):min(gen_cont_total,p*gens_per_proc)]
        network_proc["branch_contingencies"] = network_proc["branch_contingencies"][(1+(p-1)*branch_per_proc):min(branch_cont_total,p*branch_per_proc)]
    end

    for (i,network_proc) in enumerate(networks)
        info(LOGGER, "network $(i): $(length(network_proc["gen_contingencies"])), $(length(network_proc["branch_contingencies"]))")
    end

    result = pmap(solution2_solver, networks)

    contingency_solutions = Dict{String,Any}()
    for cont_sols in result
        for (id,sol) in cont_sols
            @assert !haskey(contingency_solutions, id)
            contingency_solutions[id] = sol
        end
    end

    correct_contingency_solutions!(network, contingency_solutions)
    write_solution2(network, contingency_solutions; output_dir=output_dir)

    time_contingencies = time() - time_contingencies_start
    info(LOGGER, "contingency eval time: $(time_contingencies)")
end


@everywhere function solution2_solver(network)
    bus_gens = gens_by_bus(network)

    network["delta"] = 0
    for (i,bus) in network["bus"]
        bus["vm_base"] = bus["vm"]
        bus["vm_start"] = bus["vm"]
        bus["va_start"] = bus["va"]
        bus["vm_fixed"] = length(bus_gens[i]) != 0

    end

    for (i,gen) in network["gen"]
        gen["pg_base"] = gen["pg"]
        gen["pg_start"] = gen["pg"]
        gen["qg_start"] = gen["qg"]
        gen["pg_fixed"] = false
        gen["qg_fixed"] = false
    end

    nlp_solver = JuMP.with_optimizer(Ipopt.Optimizer, tol=1e-6, print_level=0)

    contingency_solutions = Dict{String,Any}()

    for cont in network["gen_contingencies"]
        info(LOGGER, "working on: $(cont.label)")
        network_tmp = deepcopy(network)

        cont_gen = network_tmp["gen"]["$(cont.idx)"]
        cont_gen["contingency"] = true
        cont_gen["gen_status"] = 0
        pg_lost = cont_gen["pg"]

        result = run_fixpoint_pf_v2_2!(network_tmp, pg_lost, ACRPowerModel, nlp_solver, iteration_limit=5)

        result["solution"]["feasible"] = (result["termination_status"] == LOCALLY_SOLVED || result["termination_status"] == ALMOST_LOCALLY_SOLVED)
        result["solution"]["cont_type"] = "gen"
        result["solution"]["cont_comp_id"] = cont.idx

        result["solution"]["gen"]["$(cont.idx)"]["pg"] = 0.0
        result["solution"]["gen"]["$(cont.idx)"]["qg"] = 0.0

        contingency_solutions[cont.label] = result["solution"]
        network_tmp["gen"]["$(cont.idx)"]["gen_status"] = 1
    end

    for cont in network["branch_contingencies"]
        info(LOGGER, "working on: $(cont.label)")
        network_tmp = deepcopy(network)
        network_tmp["branch"]["$(cont.idx)"]["br_status"] = 0

        result = run_fixpoint_pf_v2_2!(network_tmp, 0.0, ACRPowerModel, nlp_solver, iteration_limit=5)

        result["solution"]["feasible"] = (result["termination_status"] == LOCALLY_SOLVED || result["termination_status"] == ALMOST_LOCALLY_SOLVED)
        result["solution"]["cont_type"] = "branch"
        result["solution"]["cont_comp_id"] = cont.idx

        contingency_solutions[cont.label] = result["solution"]
        network_tmp["branch"]["$(cont.idx)"]["br_status"] = 1
    end

    return contingency_solutions
end


# -- PARSER FUNCTIONS ------------------------------------------------------------------------------
@everywhere using PowerModels

##### Generic Helper Functions #####

function remove_comment(string)
    return split(string, "/")[1]
end



##### GOC Initialization File Parser (.ini) #####

function parse_goc_files(ini_file; scenario_id="")
    files, scenario_id = find_goc_files(ini_file, scenario_id=scenario_id)
    return parse_goc_files(files["con"], files["inl"], files["raw"], files["rop"], ini_file=ini_file, scenario_id=scenario_id)
end

function find_goc_files(ini_file; scenario_id="")
    files = Dict(
        "rop" => "x",
        "raw" => "x",
        "con" => "x",
        "inl" => "x"
    )

    if !endswith(ini_file, ".ini")
        warn(LOGGER, "given init file does not end with .ini, $(ini_file)")
    end

    open(ini_file) do io
        for line in readlines(io)
            line = strip(line)
            #println(line)
            if startswith(line, "[INPUTS]")
                # do nothing
            elseif startswith(line, "ROP")
                files["rop"] = strip(split(line,"=")[2])
            elseif startswith(line, "RAW")
                files["raw"] = strip(split(line,"=")[2])
            elseif startswith(line, "CON")
                files["con"] = strip(split(line,"=")[2])
            elseif startswith(line, "INL")
                files["inl"] = strip(split(line,"=")[2])
            else
                warn(LOGGER, "unknown input given in ini file: $(line)")
            end
        end
    end

    #println(files)

    ini_dir = dirname(ini_file)

    #println(ini_dir)
    scenario_dirs = [file for file in readdir(ini_dir) if isdir(joinpath(ini_dir, file))]
    scenario_dirs = sort(scenario_dirs)
    #println(scenario_dirs)

    if length(scenario_id) == 0
        scenario_id = scenario_dirs[1]
        info(LOGGER, "no scenario specified, selected directory \"$(scenario_id)\"")
    else
        if !(scenario_id in scenario_dirs)
            error(LOGGER, "$(scenario_id) not found in $(scenario_dirs)")
        end
    end

    for (id, path) in files
        if path == "."
            files[id] = ini_dir
        elseif path == "x"
            files[id] = joinpath(ini_dir, scenario_id)
        else
            error(LOGGER, "unknown file path directive $(path) for file $(id)")
        end
    end

    files["raw"] = joinpath(files["raw"], "case.raw")
    files["rop"] = joinpath(files["rop"], "case.rop")
    files["inl"] = joinpath(files["inl"], "case.inl")
    files["con"] = joinpath(files["con"], "case.con")

    return files, scenario_id
end

function parse_goc_files(con_file, inl_file, raw_file, rop_file; ini_file="", scenario_id="none")
    files = Dict(
        "rop" => rop_file,
        "raw" => raw_file,
        "con" => con_file,
        "inl" => inl_file
    )

    info(LOGGER, "Parsing Files")
    info(LOGGER, "  raw: $(files["raw"])")
    info(LOGGER, "  rop: $(files["rop"])")
    info(LOGGER, "  inl: $(files["inl"])")
    info(LOGGER, "  con: $(files["con"])")

    info(LOGGER, "skipping power models data warnings")
    setlevel!(getlogger(PowerModels), "error")
    network_model = PowerModels.parse_file(files["raw"], import_all=true)
    setlevel!(getlogger(PowerModels), "info")

    gen_cost = parse_rop_file(files["rop"])
    response = parse_inl_file(files["inl"])
    contingencies = parse_con_file(files["con"])

    return (ini_file=ini_file, scenario=scenario_id, network=network_model, cost=gen_cost, response=response, contingencies=contingencies, files=files)
end


function parse_goc_opf_files(ini_file; scenario_id="")
    files = Dict(
        "rop" => "x",
        "raw" => "x",
    )

    if !endswith(ini_file, ".ini")
        warn(LOGGER, "given init file does not end with .ini, $(ini_file)")
    end

    open(ini_file) do io
        for line in readlines(io)
            line = strip(line)
            #println(line)
            if startswith(line, "[INPUTS]")
                # do nothing
            elseif startswith(line, "ROP")
                files["rop"] = strip(split(line,"=")[2])
            elseif startswith(line, "RAW")
                files["raw"] = strip(split(line,"=")[2])
            else
                warn(LOGGER, "unknown input given in ini file: $(line)")
            end
        end
    end

    #println(files)

    ini_dir = dirname(ini_file)

    #println(ini_dir)
    scenario_dirs = [file for file in readdir(ini_dir) if isdir(joinpath(ini_dir, file))]
    scenario_dirs = sort(scenario_dirs)
    #println(scenario_dirs)

    if length(scenario_id) == 0
        scenario_id = scenario_dirs[1]
        info(LOGGER, "no scenario specified, selected directory \"$(scenario_id)\"")
    else
        if !(scenario_id in scenario_dirs)
            error(LOGGER, "$(scenario_id) not found in $(scenario_dirs)")
        end
    end

    for (id, path) in files
        if path == "."
            files[id] = ini_dir
        elseif path == "x"
            files[id] = joinpath(ini_dir, scenario_id)
        else
            error(LOGGER, "unknown file path directive $(path) for file $(id)")
        end
    end

    files["raw"] = joinpath(files["raw"], "case.raw")
    files["rop"] = joinpath(files["rop"], "case.rop")

    info(LOGGER, "Parsing Files")
    info(LOGGER, "  raw: $(files["raw"])")
    info(LOGGER, "  rop: $(files["rop"])")

    network_model = PowerModels.parse_file(files["raw"], import_all=true)
    gen_cost = parse_rop_file(files["rop"])

    return (ini_file=ini_file, scenario=scenario_id, network=network_model, cost=gen_cost, files=files)
end


##### Unit Inertia and Governor Response Data File Parser (.inl) #####

function parse_inl_file(file::String)
    open(file) do io
        return parse_inl_file(io)
    end
end

function parse_inl_file(io::IO)
    inl_list = []
    for line in readlines(io)
        #line = remove_comment(line)

        if startswith(strip(line), "0")
            debug(LOGGER, "inl file sentinel found")
            break
        end
        line_parts = split(line, ",")
        @assert length(line_parts) >= 7

        inl_data = Dict(
            "i"    => parse(Int, line_parts[1]),
            "id"   => strip(line_parts[2]),
            "h"    => strip(line_parts[3]),
            "pmax" => strip(line_parts[4]),
            "pmin" => strip(line_parts[5]),
            "r"    => parse(Float64, line_parts[6]),
            "d"    => strip(line_parts[7])
        )

        @assert inl_data["r"] >= 0.0

        #println(inl_data)
        push!(inl_list, inl_data)
    end
    return inl_list
end




##### Generator Cost Data File Parser (.rop) #####

rop_sections = [
    "mod" => "Modification Code",
    "bus_vm" => "Bus Voltage Attributes",
    "shunt_adj" => "Adjustable Bus Shunts",
    "load" => "Bus Loads",
    "load_adj" => "Adjustable Bus Load Tables",
    "gen" => "Generator Dispatch Units",
    "disptbl" => "Active Power Dispatch Tables",
    "gen_reserve" => "Generator Reserve Units",
    "qg" => "Generation Reactive Capability",
    "branch_x" => "Adjustable Branch Reactance",
    "ctbl" => "Piecewise Linear Cost Curve Tables",
    "pwc" => "Piecewise Quadratic Cost Curve Tables",
    "pec" => "Polynomial & Exponential Cost Curve Tables",
    "reserve" => "Period Reserves",
    "branch_flow" => "Branch Flows",
    "int_flow" => "Interface Flows",
    "lin_const" => "Linear Constraint Dependencies",
    "dc_const" => "Two Terminal DC Line Constraint Dependencies",
]

function parse_rop_file(file::String)
    open(file) do io
        return parse_rop_file(io)
    end
end

function parse_rop_file(io::IO)
    active_section_idx = 1
    active_section = rop_sections[active_section_idx]

    section_data = Dict()
    section_data[active_section.first] = []

    line_idx = 1
    lines = readlines(io)
    while line_idx < length(lines)
        #line = remove_comment(lines[line_idx])
        line = lines[line_idx]
        if startswith(strip(line), "0")
            debug(LOGGER, "finished reading rop section $(active_section.second) with $(length(section_data[active_section.first])) items")
            active_section_idx += 1
            if active_section_idx > length(rop_sections)
                debug(LOGGER, "finished reading known rop sections")
                break
            end
            active_section = rop_sections[active_section_idx]
            section_data[active_section.first] = []
            line_idx += 1
            continue
        end

        if active_section.first == "gen"
            push!(section_data[active_section.first], _parse_rop_gen(line))
        elseif active_section.first == "disptbl"
            push!(section_data[active_section.first], _parse_rop_pg(line))
        elseif active_section.first == "ctbl"
            pwl_line_parts = split(line, ",")
            @assert length(pwl_line_parts) >= 3

            num_pwl_lines = parse(Int, pwl_line_parts[3])
            @assert num_pwl_lines > 0

            pwl_point_lines = lines[line_idx+1:line_idx+num_pwl_lines]
            #pwl_point_lines = remove_comment.(pwl_point_lines)
            push!(section_data[active_section.first], _parse_rop_pwl(pwl_line_parts, pwl_point_lines))
            line_idx += num_pwl_lines
        else
            info(LOGGER, "skipping data line: $(line)")
        end
        line_idx += 1
    end
    return section_data
end

function _parse_rop_gen(line)
    line_parts = split(line, ",")
    @assert length(line_parts) >= 4

    data = Dict(
        "bus"     => parse(Int, line_parts[1]),
        "genid"   => strip(line_parts[2]),
        "disp"    => strip(line_parts[3]),
        "disptbl" => parse(Int, line_parts[4]),
    )

    @assert data["disptbl"] >= 0

    return data
end

function _parse_rop_pg(line)
    line_parts = split(line, ",")
    @assert length(line_parts) >= 7

    data = Dict(
        "tbl"      => parse(Int, line_parts[1]),
        "pmax"     => strip(line_parts[2]),
        "pmin"     => strip(line_parts[3]),
        "fuelcost" => strip(line_parts[4]),
        "ctyp"     => strip(line_parts[5]),
        "status"   => strip(line_parts[6]),
        "ctbl"     => parse(Int, line_parts[7]),
    )

    @assert data["tbl"] >= 0
    @assert data["ctbl"] >= 0

    return data
end

function _parse_rop_pwl(pwl_parts, point_lines)
    @assert length(pwl_parts) >= 2

    points = []

    for point_line in point_lines
        point_line_parts = split(point_line, ",")
        @assert length(point_line_parts) >= 2
        x = parse(Float64, point_line_parts[1])
        y = parse(Float64, point_line_parts[2])

        push!(points, (x=x, y=y))
    end

    data = Dict(
        "ltbl"   =>  parse(Int, pwl_parts[1]),
        "label"  => strip(pwl_parts[2]),
        "points" => points
    )

    @assert data["ltbl"] >= 0

    return data
end





##### Contingency Description Data File (.con) #####

# OPEN BRANCH FROM BUS *I TO BUS *J CIRCUIT *1CKT
branch_contigency_structure = [
    1 => "OPEN",
    2 => "BRANCH",
    3 => "FROM",
    4 => "BUS",
    #5 => "I",
    6 => "TO",
    7 => "BUS",
    #8 => "J",
    9 => "CIRCUIT",
    #10 => "CKT"
]

#=
# OPEN BRANCH FROM BUS *I TO BUS *J CIRCUIT *1CKT
branch_contigency_structure_alt = [
    1 => "OPEN",
    2 => "BRANCH",
    3 => "FROM",
    4 => "BUS",
    #5 => "I",
    6 => "TO",
    7 => "BUS",
    #8 => "J",
    9 => "CKT",
    #10 => "CKT"
]
=#

# REMOVE UNIT *ID FROM BUS *I
generator_contigency_structure = [
    1 => "REMOVE",
    2 => "UNIT",
    #3 => "ID",
    4 => "FROM",
    5 => "BUS",
    #6 => "I"
]


function parse_con_file(file::String)
    open(file) do io
        return parse_con_file(io)
    end
end

function parse_con_file(io::IO)
    con_lists = []

    tokens = []

    for line in readlines(io)
        #line_tokens = split(strip(remove_comment(line)))
        line_tokens = split(strip(line))
        #println(line_tokens)
        append!(tokens, line_tokens)
    end

    #println(tokens)

    token_idx = 1
    while token_idx <= length(tokens)
        token = tokens[token_idx]
        if token == "END"
            debug(LOGGER, "end of contingency file found")
            break
        elseif token == "CONTINGENCY"
            # start reading contingencies

            contingency_name = tokens[token_idx+1]
            debug(LOGGER, "reading contingency $(contingency_name)")

            token_idx += 2
            token = tokens[token_idx]
            remaining_tokens = length(tokens) - token_idx

            if token == "OPEN" # branch contingency case
                # OPEN BRANCH FROM BUS *I TO BUS *J CIRCUIT *1CKT

                @assert remaining_tokens >= 9
                branch_tokens = tokens[token_idx:token_idx+9]
                #println(branch_tokens)

                #if !all(branch_tokens[idx] == val for (idx, val) in branch_contigency_structure) && !all(branch_tokens[idx] == val for (idx, val) in branch_contigency_structure_alt)
                if any(branch_tokens[idx] != val for (idx, val) in branch_contigency_structure)
                    error(LOGGER, "incorrect branch contingency structure: $(branch_tokens)")
                end

                bus_i = parse(Int, branch_tokens[5])
                @assert bus_i >= 0

                bus_j = parse(Int, branch_tokens[8])
                @assert bus_j >= 0

                ckt = branch_tokens[10]

                branch_contingency = Dict(
                    "label" => contingency_name,
                    "component" => "branch",
                    "action" => "open",
                    "i" => bus_i,
                    "j" => bus_j,
                    "ckt" => ckt,
                )

                push!(con_lists, branch_contingency)

                token_idx += 9
            elseif token == "REMOVE"
                # REMOVE UNIT *ID FROM BUS *I

                @assert remaining_tokens >= 5
                generator_tokens = tokens[token_idx:token_idx+5]
                #println(generator_tokens)

                if any(generator_tokens[idx] != val for (idx, val) in generator_contigency_structure)
                    error(LOGGER, "incorrect generator contingency structure: $(generator_tokens)")
                end

                gen_id = generator_tokens[3]

                bus_i = parse(Int, generator_tokens[6])
                @assert bus_i >= 0

                generator_contingency = Dict(
                    "label" => contingency_name,
                    "component" => "generator",
                    "action" => "remove",
                    "id" => gen_id,
                    "i" => bus_i,
                )

                push!(con_lists, generator_contingency)

                token_idx += 5
            elseif token == "END"
                warn(LOGGER, "no action provided for contingency $(contingency_name)")
                token_idx -= 1
            else
                warn(LOGGER, "unrecognized token $(token)")
            end

            token_idx += 1
            token = tokens[token_idx]
            if token != "END"
                error(LOGGER, "expected END token at end of CONTINGENCY, got $(token)")
            end
        else
            warn(LOGGER, "unrecognized token $(token)")
        end
        token_idx += 1
    end

    return con_lists
end




function parse_solution1_file(file::String)
    open(file) do io
        return parse_solution1_file(io)
    end
end

function parse_solution1_file(io::IO)
    bus_data_list = []
    gen_data_list = []

    lines = readlines(io)

    # skip bus list header section
    idx = 1

    separator_count = 0
    skip_next = false

    while idx <= length(lines)
        line = lines[idx]
        if length(strip(line)) == 0
            warn(LOGGER, "skipping blank line in solution1 file ($(idx))")
        elseif skip_next
            skip_next = false
        elseif startswith(strip(line), "--")
            separator_count += 1
            skip_next = true
        else
            if separator_count == 1
                parts = split(line, ",")
                @assert length(parts) >= 4
                bus_data = (
                    bus = parse(Int, parts[1]),
                    vm = parse(Float64, parts[2]),
                    va = parse(Float64, parts[3]),
                    bcs = parse(Float64, parts[4])
                )
                push!(bus_data_list, bus_data)
            elseif separator_count == 2
                parts = split(line, ",")
                @assert length(parts) >= 4
                gen_data = (
                    bus = parse(Int, parts[1]),
                    id = strip(strip(parts[2]), ['\'', ' ']),
                    pg = parse(Float64, parts[3]),
                    qg = parse(Float64, parts[4])
                )
                push!(gen_data_list, gen_data)
            else
                warn(LOGGER, "skipping line in solution1 file ($(idx)): $(line)")
            end
        end
        idx += 1
    end

    return (bus=bus_data_list, gen=gen_data_list)
end





# -- LIBRARY FUNCTIONS -----------------------------------------------------------------------------
@everywhere using JuMP

@everywhere using PowerModels

@everywhere using InfrastructureModels

@everywhere using Memento

@everywhere const LOGGER = getlogger("GOC")

@everywhere const vm_eq_tol = 1e-4

import Statistics: mean


function build_pm_model(goc_data)
    scenario = goc_data.scenario
    network = goc_data.network

    ##### General Helpers #####
    gen_lookup = Dict(tuple(gen["source_id"][2], strip(gen["source_id"][3])) => gen for (i,gen) in network["gen"])

    branch_lookup = Dict()
    for (i,branch) in network["branch"]
        if !branch["transformer"]
            branch_id = tuple(branch["source_id"][2], branch["source_id"][3], strip(branch["source_id"][4]))
        else
            branch_id = tuple(branch["source_id"][2], branch["source_id"][3], strip(branch["source_id"][5]))
            @assert branch["source_id"][4] == 0
            @assert branch["source_id"][6] == 0
        end
        branch_lookup[branch_id] = branch
    end



    ##### Link Generator Cost Data #####

    @assert network["per_unit"]
    mva_base = network["baseMVA"]

    dispatch_tbl_lookup = Dict()
    for dispatch_tbl in goc_data.cost["disptbl"]
        dispatch_tbl_lookup[dispatch_tbl["ctbl"]] = dispatch_tbl
    end

    cost_tbl_lookup = Dict()
    for cost_tbl in goc_data.cost["ctbl"]
        cost_tbl_lookup[cost_tbl["ltbl"]] = cost_tbl
    end

    gen_cost_models = Dict()
    for gen_dispatch in goc_data.cost["gen"]
        gen_id = (gen_dispatch["bus"], strip(gen_dispatch["genid"]))
        dispatch_tbl = dispatch_tbl_lookup[gen_dispatch["disptbl"]]
        cost_tbl = cost_tbl_lookup[dispatch_tbl["ctbl"]]

        gen_cost_models[gen_id] = cost_tbl
    end

    if length(gen_cost_models) != length(network["gen"])
        error(LOGGER, "cost model data missing, network has $(length(network["gen"])) generators, the cost model has $(length(gen_cost_models)) generators")
    end

    for (gen_id, cost_model) in gen_cost_models
        pm_gen = gen_lookup[gen_id]
        pm_gen["model"] = 1
        pm_gen["model_lable"] = cost_model["label"]
        pm_gen["ncost"] = length(cost_model["points"])

        #println(cost_model["points"])
        point_list = Float64[]
        for point in cost_model["points"]
            push!(point_list, point.x/mva_base)
            push!(point_list, point.y)
        end
        pm_gen["cost"] = point_list
    end



    ##### Link Generator Participation Data #####

    if length(goc_data.response) != length(network["gen"])
        error(LOGGER, "generator response model data missing, network has $(length(network["gen"])) generators, the response model has $(length(goc_data.response)) generators")
    end

    for gen_response in goc_data.response
        gen_id = (gen_response["i"], strip(gen_response["id"]))

        pm_gen = gen_lookup[gen_id]

        pm_gen["alpha"] = gen_response["r"]
    end



    ##### Flexible Shunt Data #####

    for (i,shunt) in network["shunt"]
        if shunt["source_id"][1] == "switched shunt"
            @assert shunt["source_id"][3] == 0
            @assert shunt["gs"] == 0.0
            shunt["dispatchable"] = true

            bmin = 0.0
            bmax = 0.0
            for (n_name,b_name) in [("n1","b1"),("n2","b2"),("n3","b3"),("n4","b4"),("n5","b5"),("n6","b6"),("n7","b7"),("n8","b8")]
                if shunt[b_name] <= 0.0
                    bmin += shunt[n_name]*shunt[b_name]
                else
                    bmax += shunt[n_name]*shunt[b_name]
                end
            end
            shunt["bmin"] = bmin/mva_base
            shunt["bmax"] = bmax/mva_base
        else
            shunt["dispatchable"] = false
        end
    end



    ##### Add Contingency Lists #####

    generator_ids = []
    branch_ids = []

    for (i,cont) in enumerate(goc_data.contingencies)
        if cont["component"] == "branch"
            branch_id = (cont["i"], cont["j"], cont["ckt"])
            pm_branch = branch_lookup[branch_id]
            push!(branch_ids, (idx=pm_branch["index"], label=cont["label"]))

        elseif cont["component"] == "generator"
            gen_id = (cont["i"], cont["id"])
            pm_gen = gen_lookup[gen_id]
            push!(generator_ids, (idx=pm_gen["index"], label=cont["label"]))

        else
            error(LOGGER, "unrecognized contingency component type $(cont["component"]) at contingency $(i)")
        end
    end

    network["branch_contingencies"] = branch_ids
    network["gen_contingencies"] = generator_ids

    network["branch_contingencies_active"] = []
    network["gen_contingencies_active"] = []



    ##### Fix Broken Data #####

    PowerModels.correct_cost_functions!(network)

    for (i,shunt) in network["shunt"]
        # test checks if a "switched shunt" in the orginal data model
        if shunt["dispatchable"]
            if shunt["bs"] < shunt["bmin"]
                warn(LOGGER, "update bs on shunt $(i) to be in bounds $(shunt["bs"]) -> $(shunt["bmin"])")
                shunt["bs"] = shunt["bmin"]
            end
            if shunt["bs"] > shunt["bmax"]
                warn(LOGGER, "update bs on shunt $(i) to be in bounds $(shunt["bs"]) -> $(shunt["bmax"])")
                shunt["bs"] = shunt["bmax"]
            end
        end
    end

    return network
end


function build_pm_opf_model(goc_data)
    scenario = goc_data.scenario
    network = goc_data.network

    ##### General Helpers #####

    gen_lookup = Dict(tuple(gen["source_id"][2], strip(gen["source_id"][3])) => gen for (i,gen) in network["gen"])

    branch_lookup = Dict()
    for (i,branch) in network["branch"]
        if !branch["transformer"]
            branch_id = tuple(branch["source_id"][2], branch["source_id"][3], strip(branch["source_id"][4]))
        else
            branch_id = tuple(branch["source_id"][2], branch["source_id"][3], strip(branch["source_id"][5]))
            @assert branch["source_id"][4] == 0
            @assert branch["source_id"][6] == 0
        end
        branch_lookup[branch_id] = branch
    end



    ##### Link Generator Cost Data #####

    @assert network["per_unit"]
    mva_base = network["baseMVA"]

    dispatch_tbl_lookup = Dict()
    for dispatch_tbl in goc_data.cost["disptbl"]
        dispatch_tbl_lookup[dispatch_tbl["ctbl"]] = dispatch_tbl
    end

    cost_tbl_lookup = Dict()
    for cost_tbl in goc_data.cost["ctbl"]
        cost_tbl_lookup[cost_tbl["ltbl"]] = cost_tbl
    end

    gen_cost_models = Dict()
    for gen_dispatch in goc_data.cost["gen"]
        gen_id = (gen_dispatch["bus"], strip(gen_dispatch["genid"]))
        dispatch_tbl = dispatch_tbl_lookup[gen_dispatch["disptbl"]]
        cost_tbl = cost_tbl_lookup[dispatch_tbl["ctbl"]]

        gen_cost_models[gen_id] = cost_tbl
    end

    if length(gen_cost_models) != length(network["gen"])
        error(LOGGER, "cost model data missing, network has $(length(network["gen"])) generators, the cost model has $(length(gen_cost_models)) generators")
    end

    for (gen_id, cost_model) in gen_cost_models
        pm_gen = gen_lookup[gen_id]
        pm_gen["model"] = 1
        pm_gen["model_lable"] = cost_model["label"]
        pm_gen["ncost"] = length(cost_model["points"])

        #println(cost_model["points"])
        point_list = Float64[]
        for point in cost_model["points"]
            push!(point_list, point.x/mva_base)
            push!(point_list, point.y)
        end
        pm_gen["cost"] = point_list
    end

    PowerModels.correct_cost_functions!(network)

    return network
end



function read_solution1(network; output_dir="", state_file="solution1.txt")
    if length(output_dir) > 0
        solution1_path = joinpath(output_dir, state_file)
    else
        solution1_path = state_file
    end
    return build_pm_solution(network, solution1_path)
end

function build_pm_solution(network, goc_sol_file::String)
    info(LOGGER, "loading solution file: $(goc_sol_file)")
    goc_sol = parse_solution1_file(goc_sol_file)

    info(LOGGER, "converting GOC solution to PowerModels solution")
    pm_sol = build_pm_solution(network, goc_sol)

    return pm_sol
end

function build_pm_solution(network, goc_sol)
    bus_lookup = Dict(parse(Int, bus["source_id"][2]) => bus for (i,bus) in network["bus"])
    gen_lookup = Dict((gen["source_id"][2], strip(gen["source_id"][3])) => gen for (i,gen) in network["gen"])
    shunt_lookup = Dict{Int,Any}()
    for (i,shunt) in network["shunt"]
        if shunt["source_id"][1] == "switched shunt"
            @assert shunt["source_id"][3] == 0
            shunt_lookup[shunt["source_id"][2]] = shunt
        end
    end

    base_mva = network["baseMVA"]

    bus_data = Dict{String,Any}()
    shunt_data = Dict{String,Any}()
    for bus_sol in goc_sol.bus
        pm_bus = bus_lookup[bus_sol.bus]
        bus_data["$(pm_bus["index"])"] = Dict(
            "vm" => bus_sol.vm,
            "va" => deg2rad(bus_sol.va)
        )

        if haskey(shunt_lookup, bus_sol.bus)
            pm_shunt = shunt_lookup[bus_sol.bus]
            shunt_data["$(pm_shunt["index"])"] = Dict(
                "gs" => 0.0,
                "bs" => bus_sol.bcs/base_mva
            )
        else
            @assert bus_sol.bcs == 0.0
        end
    end

    gen_data = Dict{String,Any}()
    for gen_sol in goc_sol.gen
        pm_gen = gen_lookup[(gen_sol.bus, gen_sol.id)]
        gen_data["$(pm_gen["index"])"] = Dict(
            "pg" => gen_sol.pg/base_mva,
            "qg" => gen_sol.qg/base_mva
        )
    end

    solution = Dict(
        "per_unit" => true,
        "bus" => bus_data,
        "shunt" => shunt_data,
        "gen" => gen_data
    )

    return solution
end

@everywhere function gens_by_bus(network)
    bus_gens = Dict(i => Any[] for (i,bus) in network["bus"])
    for (i,gen) in network["gen"]
        if gen["gen_status"] != 0
            push!(bus_gens["$(gen["gen_bus"])"], gen)
        end
    end
    return bus_gens
end



@everywhere function run_fixpoint_pf_v2_2!(network, pg_lost, model_constructor, solver; iteration_limit=typemax(Int64))
    time_start = time()

    delta = apply_pg_response!(network, pg_lost)
    #delta = 0.0

    info(LOGGER, "pg lost: $(pg_lost)")
    info(LOGGER, "delta: $(network["delta"])")
    info(LOGGER, "pre-solve time: $(time() - time_start)")

    base_solution = extract_solution(network)
    base_solution["delta"] = delta
    final_result = Dict(
        "termination_status" => LOCALLY_SOLVED,
        "solution" => base_solution
    )

    bus_gens = gens_by_bus(network)

    #network["delta_start"] = network["delta"]
    for (i,gen) in network["gen"]
        gen["qg_fixed"] = false
        gen["pg_start"] = gen["pg"]
        gen["qg_start"] = gen["qg"]
    end

    for (i,bus) in network["bus"]
        if length(bus_gens[i]) == 0
            bus["vm_fixed"] = false
        else
            bus["vm_fixed"] = true
        end
        #bus["vm_start"] = bus["vm"]
        #bus["va_start"] = bus["va"]
        bus["vr_start"] = bus["vm"]*cos(bus["va"])
        bus["vi_start"] = bus["vm"]*sin(bus["va"])
    end

    time_start = time()
    result = run_fixed_pf_nbf_rect2(network, model_constructor, solver)
    info(LOGGER, "pf solve time: $(time() - time_start)")
    if result["termination_status"] == LOCALLY_SOLVED || result["termination_status"] == ALMOST_LOCALLY_SOLVED
        correct_qg!(network, result["solution"], bus_gens=bus_gens)
        PowerModels.update_data!(network, result["solution"])
        final_result = result
    else
        warn(LOGGER, "contingency pf solver FAILED with status $(result["termination_status"]) on iteration 0")
        return final_result
    end






    pg_switched = true
    qg_switched = true
    vm_switched = true

    iteration = 1
    deltas = [result["solution"]["delta"]]
    while (pg_switched || qg_switched || vm_switched) && iteration <= iteration_limit
        info(LOGGER, "obj: $(result["objective"])")
        info(LOGGER, "delta: $(result["solution"]["delta"])")
        pg_switched = false
        qg_switched = false
        vm_switched = false

        for (i,gen) in network["gen"]
            pg = gen["pg_base"] + network["delta"]*gen["alpha"]

            if gen["pg_fixed"]
                if !isapprox(gen["pmax"], gen["pmin"]) && pg < gen["pmax"] && pg > gen["pmin"]
                    gen["pg"] = pg
                    gen["pg_fixed"] = false
                    pg_switched = true
                    #info(LOGGER, "unfix pg on gen $(i)")
                end
            else
                if pg >= gen["pmax"]
                    gen["pg"] = gen["pmax"]
                    gen["pg_fixed"] = true
                    pg_switched = true
                    #info(LOGGER, "fix pg to ub on gen $(i)")
                elseif gen["pg"] <= gen["pmin"]
                    gen["pg"] = gen["pmin"]
                    gen["pg_fixed"] = true
                    pg_switched = true
                    #info(LOGGER, "fix pg to lb on gen $(i)")
                end
            end
        end

        for (i,bus) in network["bus"]
            if length(bus_gens[i]) > 0
                qg = sum(gen["qg"] for gen in bus_gens[i])
                qmin = sum(gen["qmin"] for gen in bus_gens[i])
                qmax = sum(gen["qmax"] for gen in bus_gens[i])

                if bus["vm_fixed"]
                    if qg >= qmax
                        bus["vm_fixed"] = false
                        vm_switched = true
                        for gen in bus_gens[i]
                            gen["qg"] = gen["qmax"]
                            gen["qg_fixed"] = true
                        end
                        #info(LOGGER, "fix qg to ub on bus $(i)")
                    end

                    if qg <= qmin
                        bus["vm_fixed"] = false
                        vm_switched = true
                        for gen in bus_gens[i]
                            gen["qg"] = gen["qmin"]
                            gen["qg_fixed"] = true
                        end
                        #info(LOGGER, "fix qg to lb on bus $(i)")
                    end
                else
                    if qg < qmax && qg > qmin
                        bus["vm_fixed"] = true

                        vm_switched = true
                        for gen in bus_gens[i]
                            gen["qg_fixed"] = false
                            gen["qg_start"] = gen["qg"]
                        end
                        #info(LOGGER, "fix vm to on bus $(i)")
                    end
                    if qg >= qmax && bus["vm"] > bus["vm_base"]
                        bus["vm_fixed"] = true
                        vm_switched = true
                        for gen in bus_gens[i]
                            gen["qg_fixed"] = false
                        end
                        #info(LOGGER, "fix vm to on bus $(i)")
                    end
                    if qg <= qmin && bus["vm"] < bus["vm_base"]
                        bus["vm_fixed"] = true
                        vm_switched = true
                        for gen in bus_gens[i]
                            gen["qg_fixed"] = false
                        end
                        #info(LOGGER, "fix vm to on bus $(i)")
                    end
                end
            end
        end


        for (i,gen) in network["gen"]
            gen["pg_start"] = gen["pg"]
            gen["qg_start"] = gen["qg"]
        end

        for (i,bus) in network["bus"]
            bus["vm_start"] = bus["vm"]
            bus["va_start"] = bus["va"]
        end


        if pg_switched || qg_switched || vm_switched
            info(LOGGER, "bus or gen swtiched: $iteration")
            time_start = time()
            result = run_fixed_pf_nbf_rect2(network, model_constructor, solver)
            info(LOGGER, "pf solve time: $(time() - time_start)")
            if result["termination_status"] == LOCALLY_SOLVED || result["termination_status"] == ALMOST_LOCALLY_SOLVED
                correct_qg!(network, result["solution"], bus_gens=bus_gens)
                PowerModels.update_data!(network, result["solution"])
                final_result = result
            else
                warn(LOGGER, "contingency pf solver FAILED with status $(result["termination_status"]) on iteration 0")
                return final_result
            end

            push!(deltas, result["solution"]["delta"])
            iteration += 1
            if iteration >= iteration_limit
                warn(LOGGER, "hit iteration limit")
            end
            if length(deltas) > 3 && isapprox(deltas[end-2], deltas[end])
                warn(LOGGER, "cycle detected, stopping")
                break
            end
        end
    end

    return final_result
end


@everywhere function apply_pg_response!(network, pg_delta)
    @assert (pg_delta >= 0.0)
    for (i,gen) in network["gen"]
        gen["pg_fixed"] = false
    end

    pg_total = 0.0
    for (i,gen) in network["gen"]
        if gen["gen_status"] != 0
            pg_total += gen["pg"]
        end
    end

    pg_target = pg_total + pg_delta
    #info(LOGGER, "total gen:  $(pg_total)")
    #info(LOGGER, "target gen: $(pg_target)")


    delta_est = 0.0
    while !isapprox(pg_total, pg_target)
        alpha_total = 0.0
        for (i,gen) in network["gen"]
            if gen["gen_status"] != 0 && !gen["pg_fixed"]
                alpha_total += gen["alpha"]
            end
        end
        #info(LOGGER, "alpha total: $(alpha_total)")

        delta_est += pg_delta/alpha_total
        #info(LOGGER, "detla: $(delta_est)")

        for (i,gen) in network["gen"]
            if gen["gen_status"] != 0
                pg_cont = gen["pg_base"] + delta_est*gen["alpha"]

                if pg_cont <= gen["pmin"] && gen["alpha"] < 0
                    gen["pg"] = gen["pmin"]
                    if !gen["pg_fixed"]
                        gen["pg_fixed"] = true
                        #info(LOGGER, "gen $(i) hit lb $(gen["pmin"]) with target value of $(pg_cont)")
                    end
                elseif pg_cont >= gen["pmax"] && gen["alpha"] > 0

                    gen["pg"] = gen["pmax"]
                    if !gen["pg_fixed"]
                        gen["pg_fixed"] = true
                        #info(LOGGER, "gen $(i) hit ub $(gen["pmax"]) with target value of $(pg_cont)")
                    end
                else
                    gen["pg"] = pg_cont
                end
            end
        end

        pg_total = 0.0
        for (i,gen) in network["gen"]
            if gen["gen_status"] != 0
                pg_total += gen["pg"]
            end
        end
        #pg_comp = comp_pg_response_total(network, delta_est)
        #info(LOGGER, "detla: $(delta_est)")
        #info(LOGGER, "total gen comp $(pg_comp) - gen inc $(pg_total)")
        #info(LOGGER, "total gen $(pg_total) - target gen $(pg_target)")

        pg_delta = pg_target - pg_total
    end

    network["delta"] = delta_est
    return delta_est
end


@everywhere function extract_solution(network; branch_flow=false)
    sol = Dict{String,Any}()

    sol["bus"] = Dict{String,Any}()
    for (i,bus) in network["bus"]
        bus_dict = Dict{String,Any}()
        bus_dict["va"] = get(bus, "va", 0.0)
        bus_dict["vm"] = get(bus, "vm", 1.0)
        sol["bus"][i] = bus_dict
    end

    sol["shunt"] = Dict{String,Any}()
    for (i,shunt) in network["shunt"]
        shunt_dict = Dict{String,Any}()
        shunt_dict["gs"] = get(shunt, "gs", 0.0)
        shunt_dict["bs"] = get(shunt, "bs", 0.0)
        sol["shunt"][i] = shunt_dict
    end

    sol["gen"] = Dict{String,Any}()
    for (i,gen) in network["gen"]
        gen_dict = Dict{String,Any}()
        gen_dict["pg"] = get(gen, "pg", 0.0)
        gen_dict["qg"] = get(gen, "qg", 0.0)
        sol["gen"][i] = gen_dict
    end

    if branch_flow
        sol["branch"] = Dict{String,Any}()
        for (i,branch) in network["branch"]
            branch_dict = Dict{String,Any}()
            branch_dict["pf"] = get(branch, "pf", 0.0)
            branch_dict["qf"] = get(branch, "qf", 0.0)
            branch_dict["pt"] = get(branch, "pt", 0.0)
            branch_dict["qt"] = get(branch, "qt", 0.0)
            sol["branch"][i] = branch_dict
        end
    end

    return sol
end


""

@everywhere function run_fixed_pf_nbf_rect2(file, model_constructor, solver; kwargs...)
    return run_model(file, model_constructor, solver, post_fixed_pf_nbf_rect2; solution_builder = solution_second_stage!, kwargs...)
end


""

@everywhere function post_fixed_pf_nbf_rect2(pm::GenericPowerModel)
    start_time = time()
    PowerModels.variable_voltage(pm, bounded=false)
    PowerModels.variable_active_generation(pm, bounded=false)
    PowerModels.variable_reactive_generation(pm, bounded=false)
    #PowerModels.variable_branch_flow(pm, bounded=false)
    #PowerModels.variable_dcline_flow(pm, bounded=false)

    #PowerModels.variable_branch_flow(pm, bounded=false)

    # TODO set bounds bounds on alpha and total gen capacity
    var(pm)[:delta] = @variable(pm.model, delta, base_name="delta", start=0.0)
    #var(pm)[:delta] = @variable(pm.model, delta, base_name="delta", start=ref(pm, :delta_start))
    #Memento.info(LOGGER, "post variable time: $(time() - start_time)")

    start_time = time()

    vr = var(pm, :vr)
    vi = var(pm, :vi)

    PowerModels.constraint_model_voltage(pm)

    for (i,bus) in ref(pm, :bus)
        if bus["vm_fixed"]
            @constraint(pm.model, vr[i]^2 + vi[i]^2 == bus["vm_base"]^2)
        end
    end

    for i in ids(pm, :ref_buses)
        PowerModels.constraint_theta_ref(pm, i)
    end
    #Memento.info(LOGGER, "misc constraints time: $(time() - start_time)")


    start_time = time()
    p = Dict{Tuple{Int64,Int64,Int64},GenericQuadExpr{Float64,VariableRef}}()
    q = Dict{Tuple{Int64,Int64,Int64},GenericQuadExpr{Float64,VariableRef}}()
    for (i,branch) in ref(pm, :branch)
        #PowerModels.constraint_ohms_yt_from(pm, i)
        #PowerModels.constraint_ohms_yt_to(pm, i)

        f_bus_id = branch["f_bus"]
        t_bus_id = branch["t_bus"]
        f_idx = (i, f_bus_id, t_bus_id)
        t_idx = (i, t_bus_id, f_bus_id)

        f_bus = ref(pm, :bus, f_bus_id)
        t_bus = ref(pm, :bus, t_bus_id)

        #g, b = PowerModels.calc_branch_y(branch)
        #tr, ti = PowerModels.calc_branch_t(branch)
        g = branch["g"]
        b = branch["b"]
        tr = branch["tr"]
        ti = branch["ti"]

        g_fr = branch["g_fr"]
        b_fr = branch["b_fr"]
        g_to = branch["g_to"]
        b_to = branch["b_to"]
        tm = branch["tap"]

        #p_fr  = var(pm, :p, f_idx)
        #q_fr  = var(pm, :q, f_idx)
        #p_to  = var(pm, :p, t_idx)
        #q_to  = var(pm, :q, t_idx)
        vr_fr = vr[f_bus_id] #var(pm, :vr, f_bus_id)
        vr_to = vr[t_bus_id] #var(pm, :vr, t_bus_id)
        vi_fr = vi[f_bus_id] #var(pm, :vi, f_bus_id)
        vi_to = vi[t_bus_id] #var(pm, :vi, t_bus_id)

        p[f_idx] = (g+g_fr)/tm^2*(vr_fr^2 + vi_fr^2) + (-g*tr+b*ti)/tm^2*(vr_fr*vr_to + vi_fr*vi_to) + (-b*tr-g*ti)/tm^2*(vi_fr*vr_to - vr_fr*vi_to)
        q[f_idx] = -(b+b_fr)/tm^2*(vr_fr^2 + vi_fr^2) - (-b*tr-g*ti)/tm^2*(vr_fr*vr_to + vi_fr*vi_to) + (-g*tr+b*ti)/tm^2*(vi_fr*vr_to - vr_fr*vi_to)
        p[t_idx] = (g+g_to)*(vr_to^2 + vi_to^2) + (-g*tr-b*ti)/tm^2*(vr_fr*vr_to + vi_fr*vi_to) + (-b*tr+g*ti)/tm^2*(-(vi_fr*vr_to - vr_fr*vi_to))
        q[t_idx] = -(b+b_to)*(vr_to^2 + vi_to^2) - (-b*tr+g*ti)/tm^2*(vr_fr*vr_to + vi_fr*vi_to) + (-g*tr-b*ti)/tm^2*(-(vi_fr*vr_to - vr_fr*vi_to))
    end
    #Memento.info(LOGGER, "flow expr time: $(time() - start_time)")


    start_time = time()
    pg = Dict{Int,Any}()
    qg = Dict{Int,Any}()
    for (i,gen) in ref(pm, :gen)
        if gen["pg_fixed"]
            pg[i] = gen["pg"]
            @constraint(pm.model, var(pm, :pg, i) == gen["pg"])
        else
            pg[i] = gen["pg_base"] + gen["alpha"]*delta
            @constraint(pm.model, var(pm, :pg, i) == gen["pg_base"] + gen["alpha"]*delta)
        end

        if gen["qg_fixed"]
            qg[i] = gen["qg"]
            @constraint(pm.model, var(pm, :qg, i) == gen["qg"])
        else
            qg[i] = var(pm, :qg, i)
        end
    end
    #Memento.info(LOGGER, "gen expr time: $(time() - start_time)")


    start_time = time()
    for (i,bus) in ref(pm, :bus)
        #PowerModels.constraint_kcl_shunt(pm, i)

        bus_arcs = ref(pm, :bus_arcs, i)
        bus_arcs_dc = ref(pm, :bus_arcs_dc, i)
        bus_gens = ref(pm, :bus_gens, i)
        bus_loads = ref(pm, :bus_loads, i)
        bus_shunts = ref(pm, :bus_shunts, i)

        bus_pd = Dict(k => ref(pm, :load, k, "pd") for k in bus_loads)
        bus_qd = Dict(k => ref(pm, :load, k, "qd") for k in bus_loads)

        bus_gs = Dict(k => ref(pm, :shunt, k, "gs") for k in bus_shunts)
        bus_bs = Dict(k => ref(pm, :shunt, k, "bs") for k in bus_shunts)

        #p = var(pm, :p)
        #q = var(pm, :q)
        #pg = var(pm, :pg)
        #qg = var(pm, :qg)

        @constraint(pm.model, sum(p[a] for a in bus_arcs) == sum(pg[g] for g in bus_gens) - sum(pd for pd in values(bus_pd)) - sum(gs for gs in values(bus_gs))*(vr[i]^2 + vi[i]^2))
        @constraint(pm.model, sum(q[a] for a in bus_arcs) == sum(qg[g] for g in bus_gens) - sum(qd for qd in values(bus_qd)) + sum(bs for bs in values(bus_bs))*(vr[i]^2 + vi[i]^2))
    end
    #Memento.info(LOGGER, "power balance constraint time: $(time() - start_time)")

end


""

@everywhere function solution_second_stage!(pm::GenericPowerModel, sol::Dict{String,Any})
    #start_time = time()
    PowerModels.add_setpoint_bus_voltage!(sol, pm)
    #Memento.info(LOGGER, "voltage solution time: $(time() - start_time)")

    #start_time = time()
    PowerModels.add_setpoint_generator_power!(sol, pm)
    #Memento.info(LOGGER, "generator solution time: $(time() - start_time)")

    #start_time = time()
    PowerModels.add_setpoint_branch_flow!(sol, pm)
    #Memento.info(LOGGER, "branch solution time: $(time() - start_time)")

    sol["delta"] = JuMP.value(var(pm, :delta))

    #start_time = time()
    PowerModels.add_setpoint_fixed!(sol, pm, "shunt", "bs", default_value = (item) -> item["bs"])
    #Memento.info(LOGGER, "shunt solution time: $(time() - start_time)")

    #PowerModels.print_summary(sol)
end


"fixes solution degeneracy issues when qg is a free variable, as is the case in PowerFlow"

@everywhere function correct_qg!(network, solution; bus_gens=gens_by_bus(network))
    for (i,gens) in bus_gens
        if length(gens) > 1
            gen_ids = [gen["index"] for gen in gens]
            qgs = [solution["gen"]["$(j)"]["qg"] for j in gen_ids]
            if !isapprox(abs(sum(qgs)), sum(abs.(qgs)))
                #info(LOGGER, "$(i) - $(gen_ids) - $(qgs) - output requires correction!")
                qg_total = sum(qgs)

                qg_remaining = sum(qgs)
                qg_assignment = Dict(j => 0.0 for j in gen_ids)
                for (i,gen) in enumerate(gens)
                    gen_qg = qg_remaining
                    gen_qg = max(gen_qg, gen["qmin"])
                    gen_qg = min(gen_qg, gen["qmax"])
                    qg_assignment[gen["index"]] = gen_qg
                    qg_remaining = qg_remaining - gen_qg
                    if i == length(gens) && abs(qg_remaining) > 0.0
                        qg_assignment[gen["index"]] = gen_qg + qg_remaining
                    end
                end
                #info(LOGGER, "$(qg_assignment)")
                for (j,qg) in qg_assignment
                    solution["gen"]["$(j)"]["qg"] = qg
                end

                sol_qg_total = sum(solution["gen"]["$(j)"]["qg"] for j in gen_ids)
                #info(LOGGER, "$(qg_total) - $(sol_qg_total)")
                @assert isapprox(qg_total, sol_qg_total)

                #info(LOGGER, "updated to $([solution["gen"]["$(j)"]["qg"] for j in gen_ids])")
            end
        end
    end
end


"checks feasibility criteria of contingencies, corrects when possible"
function correct_contingency_solutions!(network, contingency_solutions)
    bus_gens = gens_by_bus(network)

    cont_changes = Int64[]
    cont_vm_changes_max = [0.0]
    cont_bs_changes_max = [0.0]
    cont_pg_changes_max = [0.0]
    cont_qg_changes_max = [0.0]

    for (label, cont_sol) in contingency_solutions

        vm_changes = [0.0]
        for (i,bus) in cont_sol["bus"]
            nw_bus = network["bus"][i]

            if nw_bus["bus_type"] != 4
                if length(bus_gens[i]) > 0
                    qg = sum(cont_sol["gen"]["$(gen["index"])"]["qg"] for gen in bus_gens[i])
                    qmin = sum(gen["qmin"] for gen in bus_gens[i])
                    qmax = sum(gen["qmax"] for gen in bus_gens[i])

                    if !isapprox(abs(qmin - qmax), 0.0)
                        if qg >= qmax && bus["vm"] > nw_bus["vm"]
                            #println(qmin, " ", qg, " ", qmax)
                            #warn(LOGGER, "update qg $(qmin) $(qg) $(qmax)")
                            warn(LOGGER, "update vm on bus $(i) in contingency $(label) to match set-point $(bus["vm"]) -> $(nw_bus["vm"]) due to qg upper bound and vm direction")
                            push!(vm_changes, abs(bus["vm"] - nw_bus["vm"]))
                            bus["vm"] = nw_bus["vm"]
                        end

                        if qg <= qmin && bus["vm"] < nw_bus["vm"]
                            #println(qmin, " ", qg, " ", qmax)
                            #warn(LOGGER, "update qg $(qmin) $(qg) $(qmax)")
                            warn(LOGGER, "update vm on bus $(i) in contingency $(label) to match set-point $(bus["vm"]) -> $(nw_bus["vm"]) due to qg lower bound and vm direction")
                            push!(vm_changes, abs(bus["vm"] - nw_bus["vm"]))
                            bus["vm"] = nw_bus["vm"]
                        end
                    end
                end

                if bus["vm"] > nw_bus["vmax"]
                    warn(LOGGER, "update vm on bus $(i) in contingency $(label) to match ub $(bus["vm"]) -> $(nw_bus["vmax"]) due to out of bounds")
                    push!(vm_changes, bus["vm"] - nw_bus["vmax"])
                    bus["vm"] = nw_bus["vmax"]
                end

                if bus["vm"] < nw_bus["vmin"]
                    warn(LOGGER, "update vm on bus $(i) in contingency $(label) to match lb $(bus["vm"]) -> $(nw_bus["vmin"]) due to out of bounds")
                    push!(vm_changes, nw_bus["vmin"] - bus["vm"])
                    bus["vm"] = nw_bus["vmin"]
                end
            else
                bus["vm"] = 0.0
                bus["va"] = 0.0
            end
        end


        bs_changes = [0.0]
        for (i,shunt) in cont_sol["shunt"]
            nw_shunt = network["shunt"][i]
            if haskey(nw_shunt, "dispatchable") && nw_shunt["dispatchable"]
                @assert nw_shunt["gs"] == 0.0
                @assert haskey(nw_shunt, "bmin") && haskey(nw_shunt, "bmax")
                if shunt["bs"] > nw_shunt["bmax"]
                    warn(LOGGER, "update bs on shunt $(i) in contingency $(label) to be in bounds $(shunt["bs"]) -> $(nw_shunt["bmax"])")
                    push!(bs_changes, shunt["bs"] - nw_shunt["bmax"])
                    shunt["bs"] = nw_shunt["bmax"]
                end
                if shunt["bs"] < nw_shunt["bmin"]
                    warn(LOGGER, "update bs on shunt $(i) in contingency $(label) to be in bounds $(shunt["bs"]) -> $(nw_shunt["bmin"])")
                    push!(bs_changes, nw_shunt["bmin"] - shunt["bs"])
                    shunt["bs"] = nw_shunt["bmin"]
                end
            end
        end

        gen_id = -1
        if cont_sol["cont_type"] == "gen"
            gen_id = cont_sol["cont_comp_id"]
        end

        pg_changes = [0.0]
        qg_changes = [0.0]
        delta = cont_sol["delta"]
        for (i,gen) in cont_sol["gen"]
            nw_gen = network["gen"][i]

            if !(nw_gen["gen_status"] == 0 || (gen_id >= 0 && nw_gen["index"] == gen_id))
                bus_id = nw_gen["gen_bus"]
                nw_bus = network["bus"]["$(bus_id)"]

                if gen["qg"] < nw_gen["qmax"] && gen["qg"] > nw_gen["qmin"]
                    bus = cont_sol["bus"]["$(bus_id)"]
                    #if !isapprox(bus["vm"], nw_bus["vm"])
                    if !isapprox(bus["vm"], nw_bus["vm"], atol=vm_eq_tol/2)
                        #debug(LOGGER, "bus $(bus_id) : vm_base $(nw_bus["vm"]) - vm $(bus["vm"]) : reactive bounds $(nw_gen["qmin"]) - $(gen["qg"]) - $(nw_gen["qmax"])")
                        warn(LOGGER, "update vm on bus $(bus_id) in contingency $(label) to match base case $(bus["vm"]) -> $(nw_bus["vm"]) due to within reactive bounds")
                    end
                    bus["vm"] = nw_bus["vm"]
                end

                pg_calc = nw_gen["pg"] + nw_gen["alpha"]*delta
                pg_calc = max(pg_calc, nw_gen["pmin"])
                pg_calc = min(pg_calc, nw_gen["pmax"])

                if !isapprox(gen["pg"], pg_calc, atol=1e-5)
                    warn(LOGGER, "pg value on gen $(i) $(nw_gen["source_id"]) in contingency $(label) is not consistent with the computed value given:$(gen["pg"]) calc:$(pg_calc)")
                end

                if gen["pg"] > nw_gen["pmax"]
                    warn(LOGGER, "update pg on gen $(i) $(nw_gen["source_id"]) in contingency $(label) to match ub $(gen["pg"]) -> $(nw_gen["pmax"])")
                    push!(pg_changes, gen["pg"] - nw_gen["pmax"])
                    gen["pg"] = nw_gen["pmax"]
                end

                if gen["pg"] < nw_gen["pmin"]
                    warn(LOGGER, "update pg on gen $(i) $(nw_gen["source_id"]) in contingency $(label) to match lb $(gen["pg"]) -> $(nw_gen["pmin"])")
                    push!(pg_changes, nw_gen["pmin"] - gen["pg"])
                    gen["pg"] = nw_gen["pmin"]
                end

                if gen["qg"] > nw_gen["qmax"]
                    warn(LOGGER, "update qg on gen $(i) $(nw_gen["source_id"]) in contingency $(label) to match ub $(gen["qg"]) -> $(nw_gen["qmax"])")
                    push!(qg_changes, gen["qg"] - nw_gen["qmax"])
                    gen["qg"] = nw_gen["qmax"]
                end

                if gen["qg"] < nw_gen["qmin"]
                    warn(LOGGER, "update qg on gen $(i) $(nw_gen["source_id"]) in contingency $(label) to match lb $(gen["qg"]) -> $(nw_gen["qmin"])")
                    push!(qg_changes, nw_gen["qmin"] - gen["qg"])
                    gen["qg"] = nw_gen["qmin"]
                end
            else
                gen["pg"] = 0.0
                gen["qg"] = 0.0
            end
        end

        # test imbalance on branch conts after flow corrections
        #=
        network_tmp = deepcopy(network)
        cont_type = cont_sol["cont_type"]
        cont_idx = cont_sol["cont_comp_id"]
        network_tmp["branch"]["$(cont_idx)"]["br_status"] = 0
        PowerModels.update_data!(network_tmp, cont_sol)
        deltas = compute_power_balance_deltas!(network_tmp)
        println(deltas)
        =#

        _summary_changes(network, label, vm_changes, bs_changes, pg_changes, qg_changes)

        if length(vm_changes) > 1 || length(bs_changes) > 1 || length(pg_changes) > 1 || length(qg_changes) > 1
            push!(cont_changes, 1)
        else
            push!(cont_changes, 0)
        end

        push!(cont_vm_changes_max, maximum(vm_changes))
        push!(cont_bs_changes_max, maximum(bs_changes))
        push!(cont_pg_changes_max, maximum(pg_changes))
        push!(cont_qg_changes_max, maximum(qg_changes))
    end


    println("")

    data = [
        "========",
        "bus",
        "branch",
        "gen_cont",
        "branch_cont",
        "changes_count",
        "vm_max_max",
        "bs_max_max",
        "pg_max_max",
        "qg_max_max",
        "vm_max_mean",
        "bs_max_mean",
        "pg_max_mean",
        "qg_max_mean",
    ]
    println(join(data, ", "))

    data = [
        "DATA_CCS",
        length(network["bus"]),
        length(network["branch"]),
        length(network["gen_contingencies"]),
        length(network["branch_contingencies"]),
        sum(cont_changes),
        maximum(cont_vm_changes_max),
        maximum(cont_bs_changes_max),
        maximum(cont_pg_changes_max),
        maximum(cont_qg_changes_max),
        mean(cont_vm_changes_max),
        mean(cont_bs_changes_max),
        mean(cont_pg_changes_max),
        mean(cont_qg_changes_max),
    ]
    println(join(data, ", "))
end


function _summary_changes(network, contingency, vm_changes, bs_changes, pg_changes, qg_changes)
    println("")

    data = [
        "------------",
        "contingency",
        "bus",
        "branch",
        "gen_cont",
        "branch_cont",
        "vm_count",
        "bs_count",
        "pg_count",
        "qg_count",
        "vm_max",
        "bs_max",
        "pg_max",
        "qg_max",
        "vm_mean",
        "bs_mean",
        "pg_mean",
        "qg_mean",
    ]
    println(join(data, ", "))

    data = [
        "DATA_CHANGES",
        contingency,
        length(network["bus"]),
        length(network["branch"]),
        length(network["gen_contingencies"]),
        length(network["branch_contingencies"]),
        length(vm_changes)-1,
        length(bs_changes)-1,
        length(pg_changes)-1,
        length(qg_changes)-1,
        maximum(vm_changes),
        maximum(bs_changes),
        maximum(pg_changes),
        maximum(qg_changes),
        mean(vm_changes),
        mean(bs_changes),
        mean(pg_changes),
        mean(qg_changes),
    ]
    println(join(data, ", "))
end


function write_solution2(pm_network, contingencies; output_dir="", solution_file="solution2.txt")
    if length(output_dir) > 0
        solution_path = joinpath(output_dir, solution_file)
    else
        solution_path = solution_file
    end

    open(solution_path, "w") do sol2
        base_mva = pm_network["baseMVA"]

        for (label, cont_solution) in contingencies
            bus_switched_shunt_b = Dict(i => 0.0 for (i,bus) in pm_network["bus"])
            for (i,nw_shunt) in pm_network["shunt"]
                if nw_shunt["dispatchable"] && nw_shunt["status"] == 1
                    #@assert nw_shunt["gs"] == 0.0
                    shunt = cont_solution["shunt"][i]
                    bus_switched_shunt_b["$(nw_shunt["shunt_bus"])"] += shunt["bs"]
                end
            end

            write(sol2, "-- contingency\n")
            write(sol2, "label\n")
            write(sol2, "$(label)\n")

            write(sol2, "-- bus section\n")
            write(sol2, "bus, voltage_pu, angle, shunt_b\n")
            for (i,bus) in cont_solution["bus"]
                nw_bus = pm_network["bus"][i]
                write(sol2, "$(nw_bus["index"]), $(bus["vm"]), $(rad2deg(bus["va"])), $(base_mva*bus_switched_shunt_b[i])\n")
            end

            write(sol2, "-- generator section\n")
            write(sol2, "bus, id, mw, mvar\n")
            for (i,gen) in cont_solution["gen"]
                nw_gen = pm_network["gen"][i]
                bus_index = nw_gen["source_id"][2]
                gen_id = nw_gen["source_id"][3]
                write(sol2, "$(bus_index), $(gen_id), $(base_mva*gen["pg"]), $(base_mva*gen["qg"])\n")
            end

            write(sol2, "-- delta section\n")
            write(sol2, "delta(MW)\n")
            write(sol2, "$(base_mva*cont_solution["delta"])\n")
        end
    end
end


"checks feasibility criteria of network solution, produces an error if a problem is found"
function check_network_solution(network)
    for (i,bus) in network["bus"]
        if bus["bus_type"] != 4
            if bus["vm"] > bus["vmax"] || bus["vm"] < bus["vmin"]
                error(LOGGER, "vm on $(bus["source_id"]) is not in bounds $(bus["vmin"]) to $(bus["vmax"]), given $(bus["vm"])")
            end
        end
    end

    for (i,shunt) in network["shunt"]
        if shunt["status"] != 0
            if haskey(shunt, "dispatchable")
                if shunt["dispatchable"]
                    @assert shunt["gs"] == 0.0
                    @assert haskey(shunt, "bmin") && haskey(shunt, "bmax")
                    if shunt["bs"] > shunt["bmax"] || shunt["bs"] < shunt["bmin"]
                        error(LOGGER, "bs on $(shunt["source_id"]) is not in bounds $(shunt["bmin"]) to $(shunt["bmax"]), given $(shunt["bs"])")
                    end
                end
            end
        end
    end

    for (i,gen) in network["gen"]
        if gen["gen_status"] != 0
            if gen["pg"] > gen["pmax"] || gen["pg"] < gen["pmin"]
                error(LOGGER, "pg on gen $(gen["source_id"]) not in bounds $(gen["pmin"]) to $(gen["pmax"]), given $(gen["pg"])")
            end

            if gen["qg"] > gen["qmax"] || gen["qg"] < gen["qmin"]
                error(LOGGER, "pg on gen $(gen["source_id"]) not in bounds $(gen["qmin"]) to $(gen["qmax"]), given $(gen["qg"])")
            end
        end
    end
end


function compute_power_balance_deltas!(network)
    flows = PowerModels.calc_branch_flow_ac(network)
    PowerModels.update_data!(network, flows)

    balance = PowerModels.calc_power_balance(network)
    PowerModels.update_data!(network, balance)

    p_delta_abs = [abs(bus["p_delta"]) for (i,bus) in network["bus"] if bus["bus_type"] != 4]
    q_delta_abs = [abs(bus["q_delta"]) for (i,bus) in network["bus"] if bus["bus_type"] != 4]

    return (
        p_delta_abs_max = maximum(p_delta_abs),
        p_delta_abs_mean = mean(p_delta_abs),
        q_delta_abs_max = maximum(q_delta_abs),
        q_delta_abs_mean = mean(q_delta_abs),
    )
end

# -- CALLING FUNCTION ------------------------------------------------------------------------------
function Code2_Solver(InFile1::String, InFile2::String, InFile3::String, InFile4::String, TimeLimitInSeconds::Int64, ScoringMethod::Int64, NetworkModel::String)
    println("running MyJulia2")
    println("  $(InFile1)")
    println("  $(InFile2)")
    println("  $(InFile3)")
    println("  $(InFile4)")
    println("  $(TimeLimitInSeconds)")
    println("  $(ScoringMethod)")
    println("  $(NetworkModel)")
    compute_solution2(InFile1, InFile2, InFile3, InFile4, TimeLimitInSeconds, ScoringMethod, NetworkModel)
end
