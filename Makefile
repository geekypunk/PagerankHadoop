hadoop = hadoop

bases = LeftoverMapper LeftoverReducer NodeInputFormat Node NodeOrDouble NodeOutputFormat NodeRecordReader NodeRecordWriter TrustMapper TrustReducer PageRank MyCounter
classDir = classes
sourceDir = src
javaFiles = $(addprefix $(sourceDir)/, $(addsuffix .java, $(bases)))
classFiles = $(addprefix $(classDir)/, $(addsuffix .class, $(bases)))

pagerank : PageRank.jar
	echo $(classFiles); $(hadoop) jar PageRank.jar PageRank

PageRank.jar : $(classFiles)
	jar cvf PageRank.jar $(classDir)

$(classFiles) : $(javaFiles)
	mkdir $(classDir); javac -d $(classDir) $(javaFiles)

clean : 
	rm -r $(classDir); rm -r stage*; rm PageRank.jar;

