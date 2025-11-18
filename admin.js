
const {kafka} =  require("./client");

async function init(){
    const admin  =  kafka.admin();
    console.log("admin connnecting .....");
    admin.connect();
    console.log("admin conneting success.....");

   await  admin.createTopics({
        topics:[
            {
                topic: "rider-updates",
                numPartitions:2
            }
        ]
    })

    console.log("topic created succuessfully [rider-updates]");

    console.log("disconecting admin.......");

    admin.disconnect();
}

init();