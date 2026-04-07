###### ###### ###### ###### ###### ######
# CSV file handling
###### ###### ###### ###### ###### ######

function read_csv(file_path::AbstractString, select::Vector{Symbol} = Symbol[])::DataFrame
    @debug("Loading CSV data from $file_path")
    data = DataFrame(duckdb_read(file_path))
    if length(select) > 0
        @debug("Loading columns $select from CSV data from $file_path")
        select!(data, select)
        isempty(data) && error("Columns $select not found in $file_path")
    end
    return data
end

function read_csv(file_path::AbstractString, select::Symbol)::DataFrame
    return read_csv(file_path, [select])
end

function csv_header(path::AbstractString)
    f = open(path, "r")
    header = readline(f)
    close(f)
    header
end

macro CSV_EXT()
    return (".csv", ".csv.gz")
end

iscsv(path::AbstractString) = any(endswith.(path, @CSV_EXT))

function get_csv_files(path::AbstractString)
    return filter(x -> any(endswith.(x, @CSV_EXT)), readdir(path))
end