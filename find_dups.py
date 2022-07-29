
if __name__ == "__main__":
    count = 0
    unique = set()
    infile = open("/home/karnak/glue2/queue_states.txt","r")
    for line in infile:
        #print(line)
        try:
            (system,dateTime,jobs) = line.split("\t")
        except:
            pass
        else:
            count += 1
            unique.add((system,dateTime))
    infile.close()

    print("%d unique of %d" % (len(unique),count))

