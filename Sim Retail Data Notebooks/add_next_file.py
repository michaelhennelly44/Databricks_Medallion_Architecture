# configure catalog and schema
catalog = "workspace"
schema = "sim_retail_demo"

# function to add a file from the sample / simulated streaming volume into demo volume
def add_file(file_number):
    dbutils.fs.cp(f"/Volumes/databricks_simulated_retail_customer_data/v01/retail-pipeline/customers/stream_json/{file_number}.json", f"/Volumes/{catalog}/{schema}/customer")

    dbutils.fs.cp(f"/Volumes/databricks_simulated_retail_customer_data/v01/retail-pipeline/orders/stream_json/{file_number}.json", f"/Volumes/{catalog}/{schema}/orders")

    dbutils.fs.cp(f"/Volumes/databricks_simulated_retail_customer_data/v01/retail-pipeline/status/stream_json/{file_number}.json", f"/Volumes/{catalog}/{schema}/status")

    print(f"Added file {file_number}.json")


# function to add the next file from the list
def add_next_file():
    # get the current files from customers
    # should be the same as orders and status unless something got out of whack
    current_files = dbutils.fs.ls(f"/Volumes/{catalog}/{schema}/customer")

    #check if there are any files in the volume
    if len(current_files) == 0:
        add_file("01")
        return

    # parse file numbers out of current files
    file_numbers = [file.name.removesuffix(".json") for file in current_files]

    # get the latest file added to the volume
    last_file = max(file_numbers)

    # calculate the next file to add
    next_file = int(last_file) + 1

    # make sure text is correct then add next file
    # only goes up to 30
    if next_file <= 30:
        if next_file < 10:
            next_file = f"0{next_file}"
        add_file(next_file)
    else:
        print("All files have been added")

# add next file to target folders
add_next_file()