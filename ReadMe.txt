In this system, IoTs are sensors which generate numerical data. Based on an observation of sensor, the value of IoT data keeps on changing. Some client applications require data of IoTs passing certain threshold e.g. subscribers subscribed for a threshold value i.e. sorted data. 

Hence in this system IOTs get sorted in a network as per their data value to and also identify if there is any pattern between different types of IOT devices. If a pattern is found among different types of IOT devices, it is used to prefetch the data in a cache.

Steps:
Run CEPEngine as routers on one or more machines
Connect router to by specifying router to connect to 
Run clients and connect to routers
Run 3 Data generators or IOTs of different context profiles each to any one router
Request data of context profiles from clients
Observe the data getting pulled from IOT devices initially
Observe data getting cached at clients
Observe contexts being marked as associated
Observe data gettign prefetched for later requests