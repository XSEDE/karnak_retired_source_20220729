

import MySQLdb
import MySQLdb.cursors

class CountDups(object):
    def __init__(self):
        self.num_per_system = {}
        self.dups_per_system = {}
        self.last_jobs_per_system = {}
    
    def __str__(self):
        s = ""
        for system in self.num_per_system:
            s += "%s: %d states with %d duplicates\n" % \
                 (system,self.num_per_system[system],self.dups_per_system[system])
        return s

    def _connect(self):
        conn = MySQLdb.connect(user="karnak",db="glue2",
                                    cursorclass=MySQLdb.cursors.SSCursor)
        conn.autocommit(True)
        return conn

    def _incrementCount(self, system):
        if system not in self.num_per_system:
            self.num_per_system[system] = 0
            self.dups_per_system[system] = 0
        self.num_per_system[system] += 1

    def _sameAsLast(self, system, jobs):
        last_jobs = self.last_jobs_per_system.get(system,None)
        self.last_jobs_per_system[system] = jobs
        if jobs == last_jobs:
            return True
        else:
            return False

    def _incrementDuplicateCount(self, system):
        self.dups_per_system[system] += 1

    def run(self):
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("select * from queue_states order by time asc")
        #cursor.execute("select * from queue_states limit 1000")
        for row in cursor:
            system = row[0]
            time = row[1]
            jobs = row[2]
            self._incrementCount(system)
            if self._sameAsLast(system,jobs):
                self._incrementDuplicateCount(system)
        cursor.close()
        conn.close()

        print(self)

if __name__ == "__main__":
    c = CountDups()
    c.run()
