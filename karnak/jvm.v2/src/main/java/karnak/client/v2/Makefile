CLASS_DIR = ./classes/
JFLAGS = -cp ./classes/:./lib/httpclient-4.5.2.jar:./lib/httpcore-4.4.4.jar:./lib/commons-logging-1.2.jar  -d $(CLASS_DIR) -g -Xlint:deprecation
JAVAC = javac

JAR_CMD = jar cvf
JAR_NAME = karnak_java_client_v2.jar
DEST=./target/

.SUFFIXES: .java .class

JAR_CHDIR = -C $(CLASS_DIR) .


default:
	  mkdir -p $(CLASS_DIR)
		$(JAVAC) $(JFLAGS) *.java

jar: default makejar


makejar:
		mkdir -p $(DEST)
		$(JAR_CMD) $(DEST)/$(JAR_NAME) $(JAR_CHDIR)
#		$(JAR_CMD) $(DEST)/$(JAR_NAME) $(MANIFEST) $(JAR_CHDIR)

clean:
		rm -rf ./classes/* $(DEST)/*

classes/%.class: %.java
						        $(JAVAC) $(JFLAGS) $<
