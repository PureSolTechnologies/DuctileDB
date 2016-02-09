import {Component} from 'angular2/core';
import {ROUTER_DIRECTIVES} from 'angular2/router';

import {TwitterTimelineComponent} from './twitter-timeline.component';
import {TwitterDuctileDBTimelineComponent} from './twitter-ductiledb-timeline.component';

@Component({
	selector: 'home',
	directives: [
	  TwitterTimelineComponent,
	  TwitterDuctileDBTimelineComponent,
	  ROUTER_DIRECTIVES
	],
	template:
`<div class="container">
  <div class="row">
  </div>
  <div class="row">
    <div class="col-md-6">
      <h1>Introduction</h1>
      <p>
        DuctileDB is a graph database inspired by <a href="http://titan.thinkaurelius.com" target="_blank">Titan</a> and <a href="http://neo4j.com" target="_blank">Neo4j</a>. Combining Titan's large graph storage idea based on HBase and the rich features by Neo4j, DuctileDB goes to be an alternative graph database for very large graphs.
      </p>
      <h1>Features</h1>
      <ul>
        <li>Graph storage in <a href="http://hbase.apache.org" target="_blank">HBase</a> on <a href="http://hadoop.apache.org" target="_blank">Hadoop Distributed File System</a></li>
        <li>Graph processing based on <a href="https://tinkerpop.incubator.apache.org" target="_blank">Tinkerpop</a></li>
        <li>Integration with <a href="https://github.com/buschmais/extended-objects" target="_blank">eXtended Objects</a> as graph object mapper</li>
      </ul>
      <p>
        <a [routerLink]="['Features']">Read more...</a>
      </p>
      <h1>Get it</h1>
      <p>
        DuctileDB is hosted on <a href="https://github.com/PureSolTechnologies/DuctileDB" target="_blank">GitHub</a> to provide it and to enable it to get worked on.
        <a [routerLink]="['Contribute']">Read more...</a>	 
      </p>
      <h1>License</h1>
      <p>
        DuctileDB was published under <a href="http://www.apache.org/licenses/LICENSE-2.0.html" target="_blank">Apache License Version 2.0</a>. 
      </p>
    </div>
    <div class="col-md-6">
      <h2>News</h2>
      <twitter-ductiledb-timeline></twitter-ductiledb-timeline>
    </div>
  </div>
</div>
`
})
export class HomeComponent {
}
