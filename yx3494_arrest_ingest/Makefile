install:
	mvn wrapper:wrapper

build:
	./mvnw clean package
	cp target/NYDP_Ingestion-1.0-SNAPSHOT.jar nypd_ingestion.jar

clean:
	./mvnw clean
	rm -f *.jar

profile:
	hadoop jar nypd_ingestion.jar edu.nyu.yx3494.NYDPIngestion nypd_data/NYPD_Arrests_Data__Historic.csv nypd_data/ -p

run:
	hadoop jar nypd_ingestion.jar edu.nyu.yx3494.NYDPIngestion nypd_data/NYPD_Arrests_Data__Historic.csv nypd_data/
