import {Component} from 'angular2/core';
import {TwitterTimelineComponent} from './twitter-timeline.component';
import {TwitterDuctileDBTimelineComponent} from './twitter-ductiledb-timeline.component';

@Component({
	selector: 'home',
	directives: [
	  TwitterTimelineComponent,
	  TwitterDuctileDBTimelineComponent
	],
	template:
`<div class="container">
  <div class="row">
    <h1>Introduction</h1>
    <p>
      DuctileDB is a graph database inspired by Titan and Neo4j. Combining Titan's large graph storage idea based on HBase and the rich features by Neo4j, DuctileDB goes to be an alternative graph database for very large graphs.
    </p>
  </div>
  <div class="row">
    <div class="col-md-6">
      <h2>News</h2>
      <twitter-ductiledb-timeline></twitter-ductiledb-timeline>
    </div>
    <div class="col-md-6">
      <h2>PureSol Technologies News</h2>
      <twitter-timeline></twitter-timeline>
    </div>
  </div>
</div>
`
})
export class HomeComponent {
}
