import {Component} from 'angular2/core';
import {TwitterTimelineComponent} from './twitter-timeline.component';
import {TwitterDuctileDBTimelineComponent} from './twitter-ductiledb-timeline.component';

@Component({
	selector: 'home',
	directives: [
	  TwitterTimelineComponent,
	  TwitterDuctileDBTimelineComponent
	],
	template: `
<div class="container">
  <div class="row">
    <div class="col-md-6">
      <h1>News</h1>
      <twitter-ductiledb-timeline></twitter-ductiledb-timeline>
    </div>
    <div class="col-md-6">
      <h1>PureSol Technologies News</h1>
      <twitter-timeline></twitter-timeline>
    </div>
  </div>
</div>
`
})
export class HomeComponent {
}
