import csv
import heapq
import numpy as np

instances = []
with open("scheduling_preliminary_dataA_20180606/scheduling_preliminary_a_instance_deploy_20180606.csv")as csvfile:
    reader=csv.reader(csvfile, delimiter=',')
    for row in reader:
        instances.append({"id":row[0],"app":row[1],"machine":row[2]})

apps={}
with open("scheduling_preliminary_dataA_20180606/scheduling_preliminary_a_app_resources_20180606.csv")as csvfile:
    reader=csv.reader(csvfile,delimiter=',')
    for row in reader:
        apps[row[0]]={
            "cpu":list(map(lambda x:float(x),row[1].split("|"))),
            "memory":list(map(lambda x:float(x),row[2].split("|"))),
            "disk":float(row[3]),
            "P":int(row[4]),
            "M":int(row[5]),
            "PM":int(row[6])
        }
machines_list=[]
machines_map={}
with open("scheduling_preliminary_dataA_20180606/scheduling_preliminary_a_machine_resources_20180606.csv") as csvfile:
    reader=csv.reader(csvfile,delimiter=',')
    for row in reader:
        machines_list.append(
        {
            "id":row[0],
            "max_cpu":float(row[1]),
            "max_memory":float(row[2]),
            "max_disk":float(row[3]),
            "max_P":int(row[4]),
            "max_M":int(row[5]),
            "max_PM":int(row[6]),
            "cpu":[float(row[1]) for x in range(98)],
            "memory":[float(row[2]) for x in range(98)],
            "disk":float(row[3]),"P":int(row[4]),"M":int(row[5]),"PM":int(row[6]),
            "apps":{}
    })
        machines_map[row[0]]=machines_list[-1]


interference = {}
with open("scheduling_preliminary_dataA_20180606/scheduling_preliminary_a_app_interference_20180606.csv") as csvfile:
    reader = csv.reader(csvfile,delimiter=",")
    for row in reader:
        interference[(row[0],row[1])]=int(row[2])


def put(machine, instance, force=False):
        app_id = instance["app"]
        app = apps[app_id]
        cur = machine["apps"].get(app_id, 0)
        cur += 1
        if not force:
            if(app_id, app_id) in interference and interference[(app_id, app_id)] + 1 < cur:
                return False, "old_app,app_id"
            for old_app, num in machine["apps"].items():
                if old_app != app_id:
                    if (old_app, app_id) in interference and interference[(old_app,app_id)] - 1 < cur:
                        return False, "old_app,app_id"
                    if (app_id, old_app) in interference and interference[(app_id,old_app)] - 1 < num:
                        return False, "app_id,old_app"
            if app["disk"] > machine["disk"] or app["P"] >machine["P"] or app["PM"] > machine["PM"] or app["M"] > machine["M"]:
                return False, "disk, etc."
            for key in ["cpu", "memory"]:
                for v1,v2 in zip(app[key],machine[key]):
                    if v1 > v2:
                        return False, key + "error"
        instance["machine"] = machine["id"]
        for key in ["disk", "P", "M", "PM"]:
            machine[key] -= app[key]
        for key in ["cpu", "memory"]:
            for i in range(len(app[key])):
                machine[key][i] -= app[key][i]
        machine["apps"][app_id] = machine["apps"].get(app_id, 0) + 1
        return True, ""

rearrange = []

for i in instances:
    if len(i["machine"]) != 0:
        if not put(machines_map[i["machine"]], i)[0]:
            rearrange.append(i)

cnt = 0
tmp = 0
with open("result.csv","w") as f:
    #重新安排
    for i in rearrange:
        tmp += 1
        for j in machines_list:
            if len(j["apps"]) != 0:
                continue
            put(j,i)
            cnt += 1
            print("Success:",cnt,"instances:",i["machine"])
            f.write("%s,%s\n" %(i["id"],i["machine"]))
            break

    machines_pair = list(map(lambda x: (-np.mean(x["cpu"]),x["id"],x),machines_list))
    heapq.heapify(machines_pair)

    instances.sort(key=lambda x:(-apps[x["app"]]["disk"] if np.max(apps[x["app"]]["disk"]) >=200 else 0,
    -np.mean(apps[x["app"]]["cpu"]),x["id"]))

    for i in instances:
        if len(i["machine"]) != 0:
            continue
        tmp += 1


        cache=[]
        while len(machines_pair) !=0:
            cpu,id,machine = heapq.heappop(machines_pair)
            if put(machine,i)[0]:
                cnt += 1
                print("Success:",cnt,"instances:",i["machine"])
                f.write("%s,%s\n" % (i["id"],i["machine"]))
                cache.append((-np.mean(machine["cpu"]),machine["id"],machine))
                break
            else:
                cache.append((cpu,id,machine))

        for item in  cache:
            heapq.heappush(machines_pair,item)

        print(i["machine"])

        if len(i["machine"]) ==0:
            print(len(machines_pair))


            ans={}
            for index in range(len(machines_list)):
                machine = machines_list[index]
                rst = put(machine,i)[1]
                ans[rst] = ans.get(rst,0) + 1

            print(ans)

            print(i,"fail")
            break
    print("Finished")
    print(tmp)





