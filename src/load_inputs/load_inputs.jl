function load_inputs(file_path::AbstractString; rel_path::AbstractString=dirname(file_path), lazy_load::Bool = true)::Dict{Symbol,Any}
    @debug("Loading JSON data from $file_path")
    if !isfile(file_path)
        @error "File $file_path does not exist"
        return Dict{Symbol,Any}()
    elseif isjson(file_path)
        return load_json_inputs(file_path; rel_path=rel_path, lazy_load=lazy_load)
    elseif iscsv(file_path)
        return load_csv_inputs(file_path; rel_path=rel_path, lazy_load=lazy_load)
    end
    return file_path
end