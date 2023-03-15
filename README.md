# nds
проверить выполнение программы можно из среды разработки, передав в качестве агрументов путь к файлу rule.json 
и путь к папке, где будет создан json файл с резултатом выполнения

![image](https://user-images.githubusercontent.com/98388659/225118776-492c5f1f-2f00-4efc-bfd0-53b133a5989d.png)

Я собрал проект на Maven, т.к. на старте с Gradle возникли сложности (как в последствии оказалось они были связаны версией Java), и т.к Maven для меня привычнее, я продолжил работу с ней.

я не собрал запускаемый файл jar, т.к. так и не смог разрешить ошибку ....Exception in thread "main" java.lang.IllegalAccessError: class org.apache.spark.storage.StorageUtils$ (in unnamed module @0x273e7444) cannot access class sun.nio.ch.DirectBuffer (in module java.base) because module java.base does not export sun.nio.ch to unnamed module @0x273e7444
 ..... По этой причине не создал образ Docker...
 
 У меня есть понимание, что для сборки jar в данном случае нужно использовать плагины maven, но пока я не решил задачу по их настройке
