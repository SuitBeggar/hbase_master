
HBase�ṩ��TableInputFormat��TableOutputFormat��TableMapper��TableReducer����֧��ʹ��MapReduce��ܴ���HBase�ϵ����ݣ����ṩ��TableMapReduceUtil������ʼ��һ��HBase-MapReduce�����������һ����Щ�ӿڡ�

TableInputFormat��

TableInputFormat����HBase���ݰ�Region������Ƭ������̳���TableInputFormatBase�࣬TableInputFormatBase��ʵ����InputFormat��Ĵ󲿷ֹ��ܣ�TableInputFormatֻ������������˼������ýӿڡ�TableInputFormat��ͨ��setConf�ӿڽ������á������Ҫ�Զ���HBase��InputFormat�࣬����ͨ������TableInputFormatBase��ķ������п�����

TableOutputFormat��

TableOutputFormat�ฺ��MapReduce�������������д��HBase���С�TableOutputFormat��ͬ��ͨ��setConf�����������ã���ͨ������ TableOutputFormat.OUTPUT_TABLE�����������Ŀ����


һ��ʹ��TableMapper����Hbase�����ݽ��������hdfs��

һ��ʹ��TableReducer����hdfs���������뵽Hbase.

Ҳ����TableMapper���Reducerʹ�ã�Mapper���TableReducerʹ��

