/**
 * Created with IntelliJ IDEA.
 * User: viert
 * Date: 11/29/12
 * Time: 12:20 PM
 * To change this template use File | Settings | File Templates.

 Посылаем данные в виде
 POST /api/update/hostname/graphname
 field1=value1
 field2=value2
 ...
 fieldN=valueN

 reserved field names:

  _ts - optional timestamp of arrived data

 */
package main

import "web"

func main() {
	web.Start()
}
