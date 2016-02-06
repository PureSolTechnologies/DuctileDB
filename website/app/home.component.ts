import {Component} from 'angular2/core';
import {TwitterTimelineComponent} from './twitter-timeline.component';
import {GooglePlusPostsComponent} from './google-plus-posts.component';

@Component({
	selector: 'home',
	directives: [
	  TwitterTimelineComponent,
	  GooglePlusPostsComponent
	],
	template: `
	  <h1>DuctileDB Overview</h1>
	  <twitter-timeline></twitter-timeline>
	  <google-plus-posts></google-plus-posts>
	`
})
export class HomeComponent {
}
