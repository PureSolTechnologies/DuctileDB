import {Component} from 'angular2/core';

@Component({
  selector: 'google-plus-posts',
  template:
   `<div id="google-plus-content">
      <div class="g-post"></div>
    </div>`
})
export class GooglePlusPostsComponent {
  ngAfterContentInit() {
    gapi.post.go("google-plus-content");
  }
}
