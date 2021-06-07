# 'r+' allows you to read and write to a file
with open("test.txt", "r+") as f:
    # First read the file line by line
    lines = f.readlines()

    # Go back at the start of the file
    f.seek(0)

    # Filter out and rewrite lines
    for line in lines:
        if not line.startswith(' target'):
            f.write(line)

    # Truncate the remaining of the file
    f.truncate()
