mvn install:install-file \
  -Dfile=lib/anyburl-23.1.jar \
  -DgroupId=de.unima.ki.anyburl \
  -DartifactId=anyburl \
  -Dversion=23.1 \
  -Dpackaging=jar

mvn install:install-file \
  -Dfile=lib/kiabora-1.2.0.jar \
  -DgroupId=fr.lirmm.graphik.graal \
  -DartifactId=kiabora \
  -Dversion=1.2.0 \
  -Dpackaging=jar

mvn install:install-file \
  -Dfile=lib/pure-rewriter-1.1.0.jar \
  -DgroupId=fr.lirmm.graphik.graal \
  -DartifactId=pure-rewriter \
  -Dversion=1.1.0 \
  -Dpackaging=jar

mvn install:install-file \
  -Dfile=lib/graal-api-1.3.1.jar \
  -DgroupId=fr.lirmm.graphik.graal \
  -DartifactId=api \
  -Dversion=1.3.1 \
  -Dpackaging=jar