import {Component} from 'angular2/core';

declare var gapi: any;

@Component({
  selector: 'google-plus-posts',
  template:
   `<div id="google-plus-content">
      <div class="g-post"></div>
    </div>`
})
export class GooglePlusPostsComponent {
  ngAfterContentInit() {
    if (typeof gapi !== 'undefined' && gapi != null) {
      gapi.post.go("google-plus-content");
    }
  }
}
