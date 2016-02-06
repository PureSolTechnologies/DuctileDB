import {Component} from 'angular2/core';

@Component({
  selector: 'google-plus-follow-button',
  template: `<div class="g-follow" data-annotation="bubble" data-height="20" data-href="//plus.google.com/u/0/109089016580730153277" data-rel="publisher"></div>`
})
export class GooglePlusFollowButtonComponent {
  ngAfterContentInit() {
    gapi.follow.go();
  }
}
