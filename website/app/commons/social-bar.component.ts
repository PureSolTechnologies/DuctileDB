import {Component} from 'angular2/core';

import {PureSolTechnologiesComponent} from './puresol-technologies.component';
import {TwitterFollowButtonComponent} from './twitter-follow-button.component';
import {TweetButtonComponent} from './tweet-button.component';
import {GooglePlusAddoneButtonComponent} from './google-plus-addone-button.component';
import {GooglePlusFollowButtonComponent} from './google-plus-follow-button.component';
import {GooglePlusShareButtonComponent} from './google-plus-share-button.component';
import {FacebookLikeButtonComponent} from './facebook-like-button.component';
import {FacebookFollowButtonComponent} from './facebook-follow-button.component';

@Component({
	selector: 'social-bar',
	directives: [
	  TwitterFollowButtonComponent,
	  TweetButtonComponent,
	  GooglePlusAddoneButtonComponent,
	  GooglePlusFollowButtonComponent,
	  GooglePlusShareButtonComponent,
	  FacebookLikeButtonComponent,
	  FacebookFollowButtonComponent,
	  PureSolTechnologiesComponent
	],
	template:
`<twitter-follow-button></twitter-follow-button>
<tweet-button></tweet-button><br/>
<google-plus-addone-button></google-plus-addone-button>
<google-plus-follow-button></google-plus-follow-button>
<google-plus-share-button></google-plus-share-button>
<facebook-like-button></facebook-like-button>
<facebook-follow-button></facebook-follow-button>`
})
export class SocialBarComponent {
}
