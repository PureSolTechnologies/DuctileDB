import {Component} from 'angular2/core';

@Component({
  selector: 'facebook-like-button',
  template: `
    <!-- Your like button code -->
    <div class="fb-like" data-href="https://developers.facebook.com/docs/plugins/" data-layout="standard" data-action="like" data-show-faces="false" data-share="true"></div>
    `  
})
export class FacebookLikeButtonComponent {
  ngAfterContentInit() {
//    FB.XFBML.parse();
  }
}
