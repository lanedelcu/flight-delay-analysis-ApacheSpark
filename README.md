# Flight Delay Analysis with Apache Spark, using MacOS.
The project is implemented in Java, using IntelliJ IDEA as the integrated development environment (IDE)
and Maven as the build automation and dependency management tool.

This project analyzes flight delay data using Apache Spark. It calculates:
1. Most delayed airlines.
2. Airports with the most delays.
3. Average delay by day of the week.
4. Cancellation reasons.


## **Prerequisites**

Before running the project, ensure the following are installed:
1. **Java 8 or higher**:
   - Download and install from [Adoptium](https://adoptium.net/).

2. **Apache Spark**:
   - Download Spark from the [official website](https://spark.apache.org/downloads.html).
   - Unzip the folder and set a root folder for the Spark. Keep the location in mind as we will need it later
   - Set the `SPARK_HOME` environment variable:
        * open the terminal (Cmd + space bar --> type terminal) - computer terminal will open
        * type "nano ~/.zshrc" - this will open and edit the zee shell that macOS uses(also knows as bash- previously name)
        * in the newly open window type
     export SPARK_HOME=/path/to/spark eg: export SPARK_HOME=/Users/anedelcu/apache-spark/spark-3.5.5-bin-hadoop
     export PATH=$SPARK_HOME/bin:$PATH eg: export PATH=$PATH:$SPARK_HOME/bin:$PATH
        * Save and exit (Ctrl + X, then Y, then Enter).


3. **Maven**:
   - Install Maven using Homebrew:
     ```zee shell
     brew install maven


4. **Dataset**:
   - Download the flight delay dataset (e.g., from [Kaggle](https://www.kaggle.com/datasets/usdot/flight-delays)).
   - Save the dataset as a CSV file (e.g., `flight_data.csv`).

 ## Create the Java class with the variable dependencies. Compile and package the code(create a JAR file) ##
1. Set Up Your Java Project in IntelliJ
    - a.open IntelliJ -->File --> New --> Project --> choose a project name and location. Select Maven as the build system. my project's name is flight-delay-analysis
    - b.Once it opens up, in src/main/java directory(left hand side) use the package called org.example / or you can create your own
    - c.Right click on the package --> new -->Java class. Name it FlightDelayAnalysis

 2. Configure the variable Dependencies (pom.xml)
- Because we choose Maven, a file called pom.xml was created by default.
- Go in pom.xml and all the way at the bottom, after the </property> add the following dependencies:
```
 <dependencies>
 <!-- Apache Spark Core -->
  <dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-core_2.12</artifactId>
  <version>3.5.5</version>
  </dependency>
  
  <!-- Apache Spark SQL -->
  <dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.12</artifactId>
    <version>3.5.5</version>
  </dependency>
</dependencies>   
```


3. **Build and Run the Project**
    - a. open the terminal --> navigate to the root directory of your project that contains the pom.xml file.
      - * you navigate using cd command: `cd path/to/your/project`. eg cd/apache-spark/spark-3.5.5-bin-hadoop3/java-work/flight-delay-analysis
    - b. compiling the project: run `mvn clean compile`
    - c. package the project into a JAR file: run `mvn clean package` 
      - * this command generate a JAR file from the compiled class in newly created  /target folder in our flight-delay-analysis directory
      - * Check your work by going to the root directory, and check for the jar file that should be called flight-delay-analysis-1.0-SNAPSHOT.jar

4. **Run the application**
 * Execute the JAR file with Spark submit, passing the dataset path as an argument:
 
```    
       spark-submit --class org.example.FlightDelayAnalysis \        --> specify the package, as well as the main class that contains the main method
           --master "local[*]" \                                     --> Runs Spark locally using all available CPU cores
           target/flight-delay-analysis-1.0-SNAPSHOT.jar \           --> path to the compile JAR file
           /path/to/flight_data.csv                                  --> replace path to with the actual path: /Users/anedelcu/Lavinia_Nedelcu/School/archive/flight_data.csv
```

 * during spark-submit execution : Spark runs for the duration of your job, then stop.

5. **How to START SPARK on local machine**
  
There are 3 scenarios
- a. During "spark-submit" execution
    * Spark runs for the **duration of your job ONLY**, then stop.
    * see point 4. Run the application
- b. Interactive session : turns on Spark from the shell and keeps it active until you exit the Shell
    * open the terminal (Cmd + Space bar --> type terminal)
    * in the terminal type "pyspark" and a welcome to Spark message will prompt
    * Verify if the cluster is running by checking the web UI at http://localhost:4040
    * Port 4040 is the default for the Spark UI in interactive
- c. Long running cluster(in standalone cluster): Spark stays running until you stop it.
    * Navigate to the Spark installation directory with cd command.
    * Start the master node: in the terminal, in the root directory type: "./sbin/start-master.sh"
    * Start the worker node: in the terminal, in the root directory type: ""./sbin/start-worker.sh spark://<MASTER_HOST>:7077"
    * Verify if the cluster is running by checking the web UI at http://localhost:8080
    * Port 8080 is only used by the Spark Standalone Master UI (if you manually start a cluster)

7. STOP SPARK
- a.  Stopping an Interactive Shell Session (Local)
    * Press Ctrl+C (once or twice) to terminate the session or type exit() (PySpark) -->This kills the Spark driver and releases resources.
- b. Stopping a Standalone Cluster (Master & Workers)
    * stop the master node: $SPARK_HOME/sbin/stop-master.sh
    * stop the worker node: $SPARK_HOME/sbin/stop-worker.sh
    * or stop all: $SPARK_HOME/sbin/stop-all.sh
