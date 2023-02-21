for version in 3.1.1 2.8.5 2.6.0 2.8.3
do
    sed -i "s/<hadoop.version>.*<\/hadoop.version>/<hadoop.version>$version<\/hadoop.version>/" pom.xml
    mvn package -f "pom.xml"
    if [[ $? != 0 ]]
    then
        exit 1
    fi
done