import {Component} from '@angular/core';

import {PureSolTechnologiesComponent} from './commons/puresol-technologies.component';
import {PurifinityComponent} from './commons/purifinity.component';

@Component({
	selector: 'features',
	directives: [
	  PureSolTechnologiesComponent,
	  PurifinityComponent
	],
	template:
`<div class="container">
  <div class="row">
    <h1>Features</h1>
    <p>
    Because DuctileDB is a side project of <purifinity></purifinity>, a graph database was needed to handle large graphs for multiple, large projects over a long time period. Several factors let to the decision within <puresol-technologies></puresol-technologies> to create our own database.
    </p>
  </div>
  <div class="row">
    <h2>Graph Storage for Large Graphs</h2>
    <p>
    The most important requirement for DuctileDB was to provide a storage engine for large graphs. After evaluating different possibilities, <a href="http://hbase.apache.org">HBase</a> was chosen which is based on the <a href="http://hadoop.apache.org">Hadoop Distributed File System</a>. This combination provides scalable storage space.
    </p>
    <p>
    Additionally, HBase and Hadoop provide a large ecosystem of project for big data processing like MapReduce and <a href="http://spark.apache.org">Spark</a>.
  </div>
  <div class="row">
    <h2>Fast Graph Processing</h2>
    <p>
    The second most important requirment was to provide graph processing functionality flexible and fast enough to handle the large graphs stored in HBase and Hadoop. The proven <a href="https://tinkerpop.incubator.apache.org">Tinkerpop</a> was chosen to provide this functionality. With Tinkerpop, we can provide a graph processing framework which is sufficient for DuctileDB.
    </p>
  </div>
  <div class="row">
    <h2>Graph Object Mapper Provided</h2>
    <p>
    For an easy integration in your Java projects the graph object mapper <a href="https://github.com/buschmais/extended-objects">eXtended Objects</a> is supported. This mapper is also used in <purifinity></purifinity>.
    </p>
  </div>
</div>`
})
export class FeaturesComponent {
}
