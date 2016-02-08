import {Component} from 'angular2/core';
import {ROUTER_DIRECTIVES} from 'angular2/router';

import {PureSolTechnologiesComponent} from './puresol-technologies.component';
import {TwitterFollowButtonComponent} from './twitter-follow-button.component';
import {TweetButtonComponent} from './tweet-button.component';
import {GooglePlusAddoneButtonComponent} from './google-plus-addone-button.component';
import {GooglePlusFollowButtonComponent} from './google-plus-follow-button.component';
import {GooglePlusShareButtonComponent} from './google-plus-share-button.component';
import {FacebookLikeButtonComponent} from './facebook-like-button.component';
import {FacebookFollowButtonComponent} from './facebook-follow-button.component';

@Component({
	selector: 'footer',
	directives: [
	  ROUTER_DIRECTIVES,
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
`<div class="container">
  <hr />
  <div class="row">
    <div class="col-md-4">
      <puresol-technologies></puresol-technologies>
    </div>
    <div class="col-md-4">
      <twitter-follow-button></twitter-follow-button>
      <tweet-button></tweet-button><br/>
      <google-plus-addone-button></google-plus-addone-button>
      <google-plus-follow-button></google-plus-follow-button>
      <google-plus-share-button></google-plus-share-button><br/>
      <facebook-like-button></facebook-like-button>
      <facebook-follow-button></facebook-follow-button>
    </div>
    <div class="col-md-4">
      <a [routerLink]="['Imprint']">Imprint</a>
    </div>
  </div>
</div>`
})
export class FooterComponent {
}
