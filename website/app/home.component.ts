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
        DuctileDB is a graph database inspired by <a href="http://titan.thinkaurelius.com">Titan</a> and <a href="http://neo4j.com">Neo4j</a>. Combining Titan's large graph storage idea based on HBase and the rich features by Neo4j, DuctileDB goes to be an alternative graph database for very large graphs.
      </p>
      <h1>Features</h1>
      <ul>
        <li>Graph storage in <a href="http://hbase.apache.org">HBase</a> on <a href="http://hadoop.apache.org">Hadoop Distributed File System</a></li>
        <li>Graph processing based on <a href="https://tinkerpop.incubator.apache.org">Tinkerpop</a></li>
        <li>Integration with <a href="https://github.com/buschmais/extended-objects">eXtended Objects</a> as graph object mapper</li>
      </ul>
      <a [routerLink]="['Features']">Read more...</a>
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
