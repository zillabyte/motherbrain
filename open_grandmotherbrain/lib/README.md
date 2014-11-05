# Adding non-maven JARs

Reference: 
http://stackoverflow.com/questions/364114/can-i-add-jars-to-maven-2-build-classpath-without-installing-them

* Step 1: Add jars to the `lib` folder
* Step 2: Make sure jar is named GROUP.ARTIFACT_x.y.z.jar  (where x.y.z is version)
* Step 3: Run the `install_lib_jars_to_local_maven.py` script
* Step 4: The script will output maven dependency XML. Put that into the POM. 

