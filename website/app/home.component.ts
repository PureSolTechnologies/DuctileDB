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
<h1>DuctileDB Overview</h1>
<twitter-timeline></twitter-timeline>
<div>
  <h1>News</h1>
  <twitter-ductiledb-timeline></twitter-ductiledb-timeline>
</div>
`
})
export class HomeComponent {
}
