using Pkg, Coverage

function get_coverage_data()
    Pkg.test("MacroEnergy"; coverage=true)
    return process_folder()
end

function write_coverage_data(coverage_data::Dict{String,Any})
    LCOV.writefile("coverage/lcov.info", coverage_data)
end

function run_coverage_test()
    Pkg.test("MacroEnergy"; coverage=true)    # activate the environment
    LCOV.writefile("coverage/lcov.info", process_folder())
    # Find and remove all .cov files
    for (root, _, files) in walkdir(".")
        for file in files
            if endswith(file, ".cov")
                rm(joinpath(root, file))
            end
        end
    end
end
